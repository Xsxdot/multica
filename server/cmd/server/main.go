package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/multica-ai/multica/server/internal/analytics"
	"github.com/multica-ai/multica/server/internal/channel"
	feishuadapter "github.com/multica-ai/multica/server/internal/channel/adapter/feishu"
	"github.com/multica-ai/multica/server/internal/channel/leader"
	"github.com/multica-ai/multica/server/internal/channel/outbound"
	"github.com/multica-ai/multica/server/internal/daemonws"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/logger"
	obsmetrics "github.com/multica-ai/multica/server/internal/metrics"
	"github.com/multica-ai/multica/server/internal/realtime"
	"github.com/multica-ai/multica/server/internal/service"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/redis/go-redis/v9"
)

var (
	version = "dev"
	commit  = "unknown"
)

func newNamedRedisClient(base *redis.Options, suffix string) *redis.Client {
	opts := *base
	opts.ClientName = redisClientName(opts.ClientName, suffix)
	return redis.NewClient(&opts)
}

func redisClientName(existing, suffix string) string {
	if suffix == "" {
		return existing
	}
	if existing != "" {
		return existing + ":" + suffix
	}
	return "multica-api:" + suffix
}

func closeRedisClient(label string, client *redis.Client) {
	if client == nil {
		return
	}
	if err := client.Close(); err != nil {
		slog.Warn("redis client close failed", "client", label, "error", err)
	}
}

func shardedRelayConfigFromEnv() realtime.ShardedStreamRelayConfig {
	cfg := realtime.DefaultShardedStreamRelayConfig()
	cfg.Shards = envPositiveInt("REALTIME_RELAY_SHARDS", cfg.Shards)
	cfg.StreamMaxLen = envPositiveInt64("REALTIME_RELAY_STREAM_MAXLEN", cfg.StreamMaxLen)
	cfg.ReadCount = envPositiveInt64("REALTIME_RELAY_XREAD_COUNT", cfg.ReadCount)
	cfg.ReadBlock = envDuration("REALTIME_RELAY_XREAD_BLOCK", cfg.ReadBlock)
	return cfg
}

func realtimeRelayModeFromEnv() string {
	const defaultMode = "sharded"
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("REALTIME_RELAY_MODE")))
	if raw == "" {
		return defaultMode
	}
	switch raw {
	case "sharded", "dual", "legacy":
		return raw
	default:
		slog.Warn("invalid env var, using default", "name", "REALTIME_RELAY_MODE", "value", raw, "default", defaultMode)
		return defaultMode
	}
}

func envPositiveInt(name string, def int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		slog.Warn("invalid env var, using default", "name", name, "value", raw, "default", def, "error", err)
		return def
	}
	return v
}

func envPositiveInt64(name string, def int64) int64 {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 {
		slog.Warn("invalid env var, using default", "name", name, "value", raw, "default", def, "error", err)
		return def
	}
	return v
}

func envDuration(name string, def time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	v, err := time.ParseDuration(raw)
	if err != nil || v <= 0 {
		slog.Warn("invalid env var, using default", "name", name, "value", raw, "default", def.String(), "error", err)
		return def
	}
	return v
}

func main() {
	logger.Init()

	// Warn about missing configuration
	if os.Getenv("JWT_SECRET") == "" {
		slog.Warn("JWT_SECRET is not set — using insecure default. Set JWT_SECRET for production use.")
	}
	if os.Getenv("RESEND_API_KEY") == "" {
		slog.Warn("RESEND_API_KEY is not set — email verification codes will be printed to the log instead of emailed.")
	}
	if os.Getenv("MULTICA_DEV_VERIFICATION_CODE") != "" {
		if strings.EqualFold(strings.TrimSpace(os.Getenv("APP_ENV")), "production") {
			slog.Warn("MULTICA_DEV_VERIFICATION_CODE is set but ignored because APP_ENV=production.")
		} else {
			slog.Warn("MULTICA_DEV_VERIFICATION_CODE is enabled. Use it only for local development or private test instances.")
		}
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://multica:multica@localhost:5432/multica?sslmode=disable"
	}

	// Connect to database
	ctx := context.Background()
	pool, err := newDBPool(ctx, dbURL)
	if err != nil {
		slog.Error("unable to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		slog.Error("unable to ping database", "error", err)
		os.Exit(1)
	}
	slog.Info("connected to database")
	logPoolConfig(pool)

	bus := events.New()
	hub := realtime.NewHub()
	go hub.Run()
	daemonHub := daemonws.NewHub()
	var daemonWakeup service.TaskWakeupNotifier = daemonHub

	// MUL-1138: when REDIS_URL is set, route fanout through a Redis relay so
	// multiple API nodes can deliver each other's events. Without it the hub
	// is the sole broadcaster and the server stays single-node (legacy).
	// Runtime local-skill stores and realtime relay traffic use separate Redis
	// clients so blocking stream consumers cannot starve request-path Redis
	// operations.
	relayCtx, relayCancel := context.WithCancel(context.Background())
	var broadcaster realtime.Broadcaster = hub
	var storeRedis *redis.Client
	var relayWriteRedis *redis.Client
	var relayReadRedis *redis.Client
	var shardedReadRedis *redis.Client
	var legacyReadRedis *redis.Client
	var relay realtime.ManagedRelay
	defer func() {
		if relay != nil {
			relay.Stop()
		}
		relayCancel()
		if relay != nil {
			relay.Wait()
		}
		closeRedisClient("realtime-read-legacy", legacyReadRedis)
		closeRedisClient("realtime-read-sharded", shardedReadRedis)
		closeRedisClient("realtime-read", relayReadRedis)
		closeRedisClient("realtime-write", relayWriteRedis)
		closeRedisClient("store", storeRedis)
	}()
	if redisURL := os.Getenv("REDIS_URL"); redisURL != "" {
		opts, err := redis.ParseURL(redisURL)
		if err != nil {
			slog.Error("invalid REDIS_URL — falling back to in-memory hub", "error", err)
		} else {
			storeRedis = newNamedRedisClient(opts, "store")
			relayWriteRedis = newNamedRedisClient(opts, "realtime-write")

			relayMode := realtimeRelayModeFromEnv()
			relayConfig := shardedRelayConfigFromEnv()
			switch relayMode {
			case "legacy":
				relayReadRedis = newNamedRedisClient(opts, "realtime-read")
				relay = realtime.NewRedisRelayWithClients(hub, relayWriteRedis, relayReadRedis)
				slog.Info("daemon websocket wakeup: Redis fanout disabled in legacy realtime relay mode")
			case "dual":
				shardedReadRedis = newNamedRedisClient(opts, "realtime-read-sharded")
				legacyReadRedis = newNamedRedisClient(opts, "realtime-read-legacy")
				sharded := realtime.NewShardedStreamRelay(hub, relayWriteRedis, shardedReadRedis, relayConfig)
				sharded.SetDaemonRuntimeDeliverer(daemonHub)
				legacy := realtime.NewRedisRelayWithClients(hub, relayWriteRedis, legacyReadRedis)
				relay = realtime.NewMirroredRelay(sharded, legacy)
				daemonWakeup = daemonws.NewRelayNotifier(daemonHub, sharded)
			default:
				relayReadRedis = newNamedRedisClient(opts, "realtime-read")
				sharded := realtime.NewShardedStreamRelay(hub, relayWriteRedis, relayReadRedis, relayConfig)
				sharded.SetDaemonRuntimeDeliverer(daemonHub)
				relay = sharded
				daemonWakeup = daemonws.NewRelayNotifier(daemonHub, sharded)
			}
			relay.Start(relayCtx)
			broadcaster = realtime.NewDualWriteBroadcaster(hub, relay)
			slog.Info(
				"realtime: Redis relay enabled",
				"node_id", relay.NodeID(),
				"mode", relayMode,
				"shards", relayConfig.Shards,
				"stream_max_len", relayConfig.StreamMaxLen,
				"xread_count", relayConfig.ReadCount,
				"xread_block", relayConfig.ReadBlock.String(),
				"store_pool_size", opts.PoolSize,
				"realtime_write_pool_size", opts.PoolSize,
				"realtime_read_pool_size", opts.PoolSize,
			)
		}
	} else {
		slog.Info("realtime: REDIS_URL not set — using in-memory hub (single-node mode)")
	}
	registerListeners(bus, broadcaster)

	// M1-T1: channel port registry — assembly point for external messaging adapters.
	channelRegistry := channel.NewRegistry()

	// M1-T7: Postgres advisory-lock leader election.
	//
	// Single-writer coordination for adapters whose upstream platforms
	// (e.g. Feishu WS) deliver events to exactly one subscriber. The
	// elector picks one replica per advisory-lock id; non-leaders sit in
	// a 5s ping loop until the leader's connection drops.
	//
	// STA-47: Wire the real Feishu SDK client. The OnAcquire callback
	// creates a new adapter with a real SDKClient backed by
	// larksuite/oapi-sdk-go/v3, registers it in the channel registry,
	// and connects. OnRelease disconnects and unregisters.
	leaderCtx, leaderCancel := context.WithCancel(context.Background())
	channelLeader := leader.NewElector(pool, leader.ChannelFeishuLockID, 5*time.Second)

	// Read Feishu credentials from environment. These are required for
	// the real SDK client. If not set, the leader election still runs
	// but the adapter will not be wired (operators can observe leader
	// rotation in pg_locks without Feishu credentials).
	feishuAppID := os.Getenv("FEISHU_APP_ID")
	feishuAppSecret := os.Getenv("FEISHU_APP_SECRET")
	feishuEncryptKey := os.Getenv("FEISHU_ENCRYPT_KEY")
	feishuVerifyToken := os.Getenv("FEISHU_VERIFY_TOKEN")
	feishuEnabled := feishuAppID != "" && feishuAppSecret != ""
	channelStorage := newStorageFromEnv()

	// adapterCancel is cancelled on leader release to stop the WS client.
	var adapterCancel context.CancelFunc
	var channelAdapterReady atomic.Bool

	channelLeader.OnAcquire(func(ctx context.Context) error {
		slog.Info("channel leader: acquired", "lock_id", leader.ChannelFeishuLockID)

		if !feishuEnabled {
			slog.Warn("channel leader: FEISHU_APP_ID / FEISHU_APP_SECRET not set; skipping feishu adapter wiring")
			return nil
		}

		// Create a context that survives beyond the OnAcquire call so
		// the WS client keeps running until OnRelease cancels it.
		adapterCtx, cancel := context.WithCancel(context.Background())
		adapterCancel = cancel

		sdkClient := feishuadapter.NewRealClient(feishuAppID, feishuAppSecret, feishuEncryptKey, feishuVerifyToken)
		adapter := feishuadapter.NewAdapter(sdkClient, feishuadapter.Config{
			AppID:       feishuAppID,
			AppSecret:   feishuAppSecret,
			EncryptKey:  feishuEncryptKey,
			VerifyToken: feishuVerifyToken,
		})

		if err := channelRegistry.Register(adapter); err != nil {
			if err == channel.ErrDuplicateChannel {
				// Already registered (e.g. from a previous acquire).
				// Get the existing one and connect it.
				existing, getErr := channelRegistry.Get("feishu")
				if getErr != nil {
					slog.Error("channel leader: failed to get existing feishu adapter", "error", getErr)
					return fmt.Errorf("feishu: get existing adapter: %w", getErr)
				}
				if connErr := existing.Connect(adapterCtx); connErr != nil {
					slog.Error("channel leader: failed to connect existing feishu adapter", "error", connErr)
					return fmt.Errorf("feishu: connect existing: %w", connErr)
				}
				channelAdapterReady.Store(true)
				slog.Info("channel leader: reconnected existing feishu adapter")
				return nil
			}
			slog.Error("channel leader: failed to register feishu adapter", "error", err)
			return fmt.Errorf("feishu: register adapter: %w", err)
		}

		if err := adapter.Connect(adapterCtx); err != nil {
			slog.Error("channel leader: failed to connect feishu adapter", "error", err)
			// Unregister on failure so the next acquire can try again.
			_ = channelRegistry.Unregister("feishu")
			return fmt.Errorf("feishu: connect: %w", err)
		}
		channelAdapterReady.Store(true)

		var pipelineOpts []channelPipelineOptions
		if channelStorage != nil {
			pipelineOpts = append(pipelineOpts, channelPipelineOptions{
				Storage:        channelStorage,
				FileDownloader: feishuadapter.NewRealFileDownloader(sdkClient.APIClient()),
			})
		} else {
			slog.Info("channel attachment step disabled: storage is not configured")
		}
		pipeline := newChannelInboundPipeline(pool, channelRegistry, pipelineOpts...)
		go func() {
			for {
				select {
				case <-adapterCtx.Done():
					return
				case evt, ok := <-adapter.Events():
					if !ok {
						return
					}
					outcome, err := pipeline.Run(adapterCtx, evt)
					if err != nil {
						slog.Error("channel inbound pipeline failed",
							"channel", evt.ChannelName,
							"chat_id", evt.ChatID,
							"event_id", evt.EventID,
							"terminal", outcome.Terminal,
							"error", err,
						)
						continue
					}
					slog.Debug("channel inbound pipeline completed",
						"channel", evt.ChannelName,
						"chat_id", evt.ChatID,
						"event_id", evt.EventID,
						"terminal", outcome.Terminal,
						"decision", outcome.Decision,
					)
				}
			}
		}()

		slog.Info("channel leader: feishu adapter connected", "app_id", feishuAppID)
		return nil
	})
	channelLeader.OnRelease(func(ctx context.Context) error {
		slog.Info("channel leader: released", "lock_id", leader.ChannelFeishuLockID)
		channelAdapterReady.Store(false)

		// Cancel the adapter context to stop the WS client.
		if adapterCancel != nil {
			adapterCancel()
		}

		// Disconnect the adapter if registered.
		adapter, err := channelRegistry.Get("feishu")
		if err != nil {
			// Not registered — nothing to disconnect.
			return nil
		}
		if discErr := adapter.Disconnect(ctx); discErr != nil {
			slog.Error("channel leader: feishu adapter disconnect failed", "error", discErr)
		}
		// Unregister so the next acquire starts fresh.
		_ = channelRegistry.Unregister("feishu")
		return nil
	})
	go func() {
		if err := channelLeader.Run(leaderCtx); err != nil {
			// Run only returns non-nil on a structural error (pool /
			// callback misbehaviour). Log and continue — the server
			// stays up; the elector is best-effort coordination, not
			// a critical-path dependency for HTTP traffic.
			slog.Error("channel leader: run terminated with error", "error", err)
		}
	}()

	analyticsClient := analytics.NewFromEnv()
	defer analyticsClient.Close()

	queries := db.New(pool)
	hub.SetAuthorizer(newScopeAuthorizer(queries))
	// Order matters: subscriber listeners must register BEFORE notification listeners.
	// The notification listener queries the subscriber table to determine recipients,
	// so subscribers must be written first within the same synchronous event dispatch.
	registerSubscriberListeners(bus, queries)
	registerActivityListeners(bus, queries)
	registerNotificationListeners(bus, queries)

	var channelOutboundSubscriber *outbound.Subscriber
	if feishuEnabled {
		outboundChannel := newRegistryChannel(channelRegistry, "feishu")
		cardSender := outbound.NewFailureRecordingCardSender(outboundChannel, queries)
		cardSender.SetActiveFunc(channelAdapterReady.Load)
		channelOutboundSubscriber = outbound.NewSubscriber(
			bus,
			outboundChannel,
			outbound.NewDBBindingStore(pool),
			outbound.NewDBPrefStore(queries),
			"",
		)
		channelOutboundSubscriber.SetActiveFunc(channelAdapterReady.Load)
		channelOutboundSubscriber.SetFailureRecorder(queries)
		channelOutboundSubscriber.SetAggregator(outbound.NewAggregator(
			cardSender,
			outbound.DefaultFlushInterval,
		))
		channelOutboundSubscriber.Start()
		slog.Info("channel outbound subscriber started", "provider", "feishu")
	} else {
		slog.Info("channel outbound subscriber disabled: Feishu credentials are not configured")
	}

	metricsConfig := obsmetrics.ConfigFromEnv()
	var metricsServer *http.Server
	var httpMetrics *obsmetrics.HTTPMetrics
	if metricsConfig.Enabled() {
		metricsRegistry := obsmetrics.NewRegistry(obsmetrics.RegistryOptions{
			Pool:     pool,
			Realtime: realtime.M,
			DaemonWS: daemonws.M,
			Version:  version,
			Commit:   commit,
		})
		httpMetrics = metricsRegistry.HTTP
		metricsServer = obsmetrics.NewServer(metricsConfig.Addr, metricsRegistry.Gatherer)
		if !obsmetrics.IsLoopbackAddr(metricsConfig.Addr) {
			slog.Warn(
				"metrics listener is not loopback-only; restrict access with private networking, allowlists, or proxy auth",
				"addr", metricsConfig.Addr,
			)
		}
	}

	r := NewRouterWithOptions(pool, hub, bus, analyticsClient, storeRedis, RouterOptions{
		HTTPMetrics:  httpMetrics,
		DaemonHub:    daemonHub,
		DaemonWakeup: daemonWakeup,
		Storage:      channelStorage,
	})

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: r,
	}

	// Start background workers.
	sweepCtx, sweepCancel := context.WithCancel(context.Background())
	autopilotCtx, autopilotCancel := context.WithCancel(context.Background())
	taskSvc := service.NewTaskService(queries, pool, hub, bus, daemonWakeup)
	autopilotSvc := service.NewAutopilotService(queries, pool, bus, taskSvc)
	registerAutopilotListeners(bus, autopilotSvc)

	// Start background sweeper to mark stale runtimes as offline.
	go runRuntimeSweeper(sweepCtx, queries, taskSvc, bus)
	go runAutopilotScheduler(autopilotCtx, queries, autopilotSvc)
	go runDBStatsLogger(sweepCtx, pool)

	// Start outbound retry + cleanup workers (T15).
	var retryCancel context.CancelFunc
	if !feishuEnabled {
		slog.Info("channel outbound retry worker disabled: Feishu credentials are not configured")
	} else if outbound.RetryWorkerEnabled() {
		retryCtx, cancel := context.WithCancel(context.Background())
		retryCancel = cancel
		retryWorker := outbound.NewRetryWorker(pool, queries, newRegistryRetrySender(channelRegistry))
		retryWorker.SetActiveFunc(channelAdapterReady.Load)
		go retryWorker.Run(retryCtx)
		if os.Getenv("CHANNEL_RETRY_WORKER_ENABLED") == "" {
			slog.Info("channel outbound retry worker started by default")
		} else {
			slog.Info("channel outbound retry worker started by env")
		}
	} else {
		slog.Info("channel outbound retry worker disabled by CHANNEL_RETRY_WORKER_ENABLED=false")
	}
	cleanupCtx, cleanupCancel := context.WithCancel(context.Background())
	cleanupWorker := outbound.NewCleanupWorker(queries)
	go cleanupWorker.Run(cleanupCtx)

	if metricsServer != nil {
		go func() {
			slog.Info("metrics server starting", "addr", metricsConfig.Addr)
			if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("metrics server disabled after startup error", "error", err)
			}
		}()
	}

	go func() {
		slog.Info("server starting", "port", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down server")
	sweepCancel()
	autopilotCancel()
	if retryCancel != nil {
		retryCancel()
	}
	if channelOutboundSubscriber != nil {
		channelOutboundSubscriber.Stop()
	}
	cleanupCancel()
	// Stop the channel leader before HTTP shutdown: OnRelease may want
	// to send a final "going down" message via the still-up service
	// layer; doing it after API shutdown would race the http.Server's
	// drain.
	leaderCancel()

	apiShutdownCtx, apiShutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := srv.Shutdown(apiShutdownCtx); err != nil {
		apiShutdownCancel()
		slog.Error("server forced to shutdown", "error", err)
		os.Exit(1)
	}
	apiShutdownCancel()

	if metricsServer != nil {
		metricsShutdownCtx, metricsShutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := metricsServer.Shutdown(metricsShutdownCtx); err != nil {
			slog.Error("metrics server forced to shutdown", "error", err)
		}
		metricsShutdownCancel()
	}
	slog.Info("server stopped")
}
