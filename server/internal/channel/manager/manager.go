package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/multica-ai/multica/server/internal/channel"
	"github.com/multica-ai/multica/server/internal/channel/inbound"
	chintent "github.com/multica-ai/multica/server/internal/channel/intent"
	"github.com/multica-ai/multica/server/internal/channel/leader"
	channelmetrics "github.com/multica-ai/multica/server/internal/channel/metrics"
	"github.com/multica-ai/multica/server/internal/channel/outbound"
	"github.com/multica-ai/multica/server/internal/channel/port"
	"github.com/multica-ai/multica/server/internal/channel/provider"
	"github.com/multica-ai/multica/server/internal/events"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type RuntimeComponents struct {
	PrePipeline   *inbound.Pipeline
	PostPipeline  *inbound.Pipeline
	RuleResolvers []chintent.IntentResolver
	ChatIntent    chintent.AsyncChatIntentClient
}

type RuntimeBuilder func(fileDownloader inbound.FileDownloader) RuntimeComponents

type Config struct {
	Pool           *pgxpool.Pool
	Queries        *db.Queries
	Bus            *events.Bus
	Registry       *channel.Registry
	Factories      []provider.Factory
	RuntimeBuilder RuntimeBuilder

	ConversationLimit      int
	GlobalLimit            int
	Workers                int
	ClaimBatch             int
	IntentTaskTimeout      time.Duration
	ActionTaskTimeout      time.Duration
	ClarificationTimeout   time.Duration
	ProcessingLease        time.Duration
	RetryWorkerEnabled     bool
	NotificationOutbox     bool
	OutboundCleanupEnabled bool
}

type Manager struct {
	cfg Config

	mu          sync.Mutex
	started     bool
	cancels     []context.CancelFunc
	subscribers []*outbound.Subscriber
	ready       map[string]*atomic.Bool
}

func New(cfg Config) *Manager {
	return &Manager{cfg: cfg, ready: make(map[string]*atomic.Bool)}
}

func (m *Manager) Start(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started {
		return
	}
	m.started = true

	connections := m.connections(ctx)
	if len(connections) == 0 {
		slog.Info("channel manager: no enabled channel providers")
		return
	}

	for _, entry := range connections {
		ready := &atomic.Bool{}
		m.ready[entry.config.ConnectionID] = ready
		m.startOutbound(entry.config, ready)
		m.startInbound(ctx, entry.factory, entry.config, ready)
	}
	m.startOutbox(ctx)
}

func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, cancel := range m.cancels {
		cancel()
	}
	m.cancels = nil
	for _, sub := range m.subscribers {
		sub.Stop()
	}
	m.subscribers = nil
	m.started = false
}

func (m *Manager) IsReady(connectionID string) bool {
	m.mu.Lock()
	ready := m.ready[connectionID]
	m.mu.Unlock()
	return ready != nil && ready.Load()
}

type envConnection struct {
	factory provider.Factory
	config  provider.ConnectionConfig
}

func (m *Manager) connections(ctx context.Context) []envConnection {
	if m.cfg.Queries != nil {
		rows, err := m.cfg.Queries.ListEnabledChannelConnections(ctx)
		if err != nil {
			slog.Error("channel manager: list configured connections failed; falling back to env", "error", err)
		} else if len(rows) > 0 {
			return m.dbConnections(rows)
		}
	}
	return m.envConnections()
}

func (m *Manager) dbConnections(rows []db.ChannelConnection) []envConnection {
	factories := make(map[string]provider.Factory, len(m.cfg.Factories))
	for _, factory := range m.cfg.Factories {
		factories[factory.Provider()] = factory
	}
	out := make([]envConnection, 0, len(rows))
	for _, row := range rows {
		factory := factories[row.Provider]
		if factory == nil {
			slog.Warn("channel manager: configured provider has no factory", "provider", row.Provider, "connection_id", row.ID)
			continue
		}
		envCfg := factory.EnvConfig()
		values := make(map[string]string, len(envCfg.Values))
		for key, value := range envCfg.Values {
			values[key] = value
		}
		if len(row.Config) > 0 {
			configured := map[string]string{}
			if err := json.Unmarshal(row.Config, &configured); err != nil {
				slog.Error("channel manager: invalid connection config", "provider", row.Provider, "connection_id", row.ID, "error", err)
				continue
			}
			for key, value := range configured {
				values[key] = value
			}
		}
		out = append(out, envConnection{
			factory: factory,
			config: provider.ConnectionConfig{
				Provider:     row.Provider,
				ConnectionID: row.ID,
				DisplayName:  row.DisplayName,
				Enabled:      row.Enabled,
				Values:       values,
			},
		})
	}
	return out
}

func (m *Manager) envConnections() []envConnection {
	out := make([]envConnection, 0, len(m.cfg.Factories))
	for _, factory := range m.cfg.Factories {
		cfg := factory.EnvConfig()
		if cfg.Provider == "" {
			cfg.Provider = factory.Provider()
		}
		if cfg.DisplayName == "" {
			cfg.DisplayName = factory.DisplayName()
		}
		if cfg.ConnectionID == "" {
			cfg.ConnectionID = cfg.Provider
		}
		if !cfg.Enabled {
			slog.Info("channel manager: provider disabled", "provider", cfg.Provider, "display_name", cfg.DisplayName)
			continue
		}
		out = append(out, envConnection{factory: factory, config: cfg})
	}
	return out
}

func (m *Manager) startInbound(ctx context.Context, factory provider.Factory, cfg provider.ConnectionConfig, ready *atomic.Bool) {
	lockID, needsLeader := factory.LeaderLockID(cfg)
	if needsLeader {
		elector := leader.NewElector(m.cfg.Pool, lockID, 5*time.Second)
		var adapterCancel context.CancelFunc
		elector.OnAcquire(func(acquireCtx context.Context) error {
			channelmetrics.M.SetLeaderState(cfg.Provider, true)
			channelmetrics.M.SetAdapterConnected(cfg.Provider, false)
			slog.Info("channel manager: leader acquired", "provider", cfg.Provider, "lock_id", lockID)

			adapterCtx, cancel := context.WithCancel(context.Background())
			adapterCancel = cancel
			go m.runAdapterLoop(adapterCtx, factory, cfg, ready)
			return nil
		})
		elector.OnRelease(func(releaseCtx context.Context) error {
			slog.Info("channel manager: leader released", "provider", cfg.Provider, "lock_id", lockID)
			ready.Store(false)
			channelmetrics.M.SetAdapterConnected(cfg.Provider, false)
			channelmetrics.M.SetLeaderState(cfg.Provider, false)
			if adapterCancel != nil {
				adapterCancel()
			}
			if ch, err := m.cfg.Registry.Get(cfg.ConnectionID); err == nil {
				if discErr := ch.Disconnect(releaseCtx); discErr != nil {
					slog.Error("channel manager: adapter disconnect failed", "provider", cfg.Provider, "error", discErr)
				}
				_ = m.cfg.Registry.Unregister(cfg.ConnectionID)
			}
			return nil
		})
		leaderCtx, cancel := context.WithCancel(ctx)
		m.cancels = append(m.cancels, cancel)
		go func() {
			if err := elector.Run(leaderCtx); err != nil {
				slog.Error("channel manager: leader terminated", "provider", cfg.Provider, "error", err)
			}
		}()
		return
	}

	adapterCtx, cancel := context.WithCancel(ctx)
	m.cancels = append(m.cancels, cancel)
	go m.runAdapterLoop(adapterCtx, factory, cfg, ready)
}

func (m *Manager) runAdapterLoop(ctx context.Context, factory provider.Factory, cfg provider.ConnectionConfig, ready *atomic.Bool) {
	delay := factory.ReconnectDelay(cfg)
	if delay <= 0 {
		delay = 5 * time.Second
	}
	for {
		if ctx.Err() != nil {
			return
		}
		ready.Store(false)
		channelmetrics.M.SetAdapterConnected(cfg.Provider, false)

		bundle, err := factory.Build(ctx, cfg)
		if err != nil {
			slog.Error("channel manager: build adapter failed", "provider", cfg.Provider, "error", err)
			waitReconnect(ctx, delay)
			continue
		}
		baseAdapter := bundle.Channel
		if baseAdapter == nil {
			slog.Error("channel manager: provider returned nil adapter", "provider", cfg.Provider)
			waitReconnect(ctx, delay)
			continue
		}
		adapter := newConnectionChannel(cfg.ConnectionID, baseAdapter)

		_ = m.cfg.Registry.Unregister(cfg.ConnectionID)
		if err := m.cfg.Registry.Register(adapter); err != nil {
			slog.Error("channel manager: register adapter failed", "provider", cfg.Provider, "error", err)
			waitReconnect(ctx, delay)
			continue
		}
		if err := adapter.Connect(ctx); err != nil {
			slog.Error("channel manager: connect adapter failed; will retry", "provider", cfg.Provider, "error", err)
			_ = m.cfg.Registry.Unregister(cfg.ConnectionID)
			waitReconnect(ctx, delay)
			continue
		}

		ready.Store(true)
		channelmetrics.M.SetAdapterConnected(cfg.Provider, true)
		slog.Info("channel manager: adapter connected", "provider", cfg.Provider, "connection_id", cfg.ConnectionID)

		reconnect := m.runInboundRuntime(ctx, cfg, bundle.FileDownloader, adapter)
		ready.Store(false)
		channelmetrics.M.SetAdapterConnected(cfg.Provider, false)
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := adapter.Disconnect(disconnectCtx); err != nil {
			slog.Warn("channel manager: adapter disconnect before reconnect failed", "provider", cfg.Provider, "error", err)
		}
		cancel()
		_ = m.cfg.Registry.Unregister(cfg.ConnectionID)
		if !reconnect {
			return
		}
		waitReconnect(ctx, delay)
	}
}

func (m *Manager) runInboundRuntime(ctx context.Context, cfg provider.ConnectionConfig, fileDownloader inbound.FileDownloader, adapter port.Channel) bool {
	if m.cfg.RuntimeBuilder == nil {
		slog.Error("channel manager: runtime builder is not configured", "provider", cfg.Provider)
		return false
	}
	components := m.cfg.RuntimeBuilder(fileDownloader)
	inboundRuntime := inbound.NewRuntime(inbound.RuntimeConfig{
		Store:                inbound.NewDBInboundEventStore(m.cfg.Pool),
		PrePipeline:          components.PrePipeline,
		PostPipeline:         components.PostPipeline,
		RuleResolvers:        components.RuleResolvers,
		ChatIntent:           components.ChatIntent,
		ReplySink:            inbound.NewRegistryReplySink(m.cfg.Registry),
		Workers:              m.cfg.Workers,
		ClaimBatch:           m.cfg.ClaimBatch,
		IntentTaskTimeout:    m.cfg.IntentTaskTimeout,
		ActionTaskTimeout:    m.cfg.ActionTaskTimeout,
		ClarificationTimeout: m.cfg.ClarificationTimeout,
		ProcessingLease:      m.cfg.ProcessingLease,
	})
	runtimeCtx, runtimeCancel := context.WithCancel(ctx)
	go inboundRuntime.Run(runtimeCtx)
	defer runtimeCancel()

	for {
		select {
		case <-ctx.Done():
			return false
		case evt, ok := <-adapter.Events():
			if !ok {
				slog.Warn("channel manager: adapter event stream closed; reconnecting", "provider", cfg.Provider)
				return true
			}
			if evt.ChannelConnectionID == "" {
				evt.ChannelConnectionID = cfg.ConnectionID
			}
			result, err := inboundRuntime.Accept(ctx, evt, inbound.AcceptOptions{
				ConversationLimit: m.cfg.ConversationLimit,
				GlobalLimit:       m.cfg.GlobalLimit,
			})
			if err != nil {
				slog.Error("channel manager: inbound accept failed",
					"provider", evt.ChannelName,
					"chat_id", evt.ChatID,
					"event_id", evt.EventID,
					"error", err,
				)
				continue
			}
			slog.Debug("channel manager: inbound event accepted",
				"provider", evt.ChannelName,
				"chat_id", evt.ChatID,
				"event_id", evt.EventID,
				"row_id", result.EventID,
				"duplicate", result.Duplicate,
				"rejected_backpressure", result.RejectedBackpressure,
				"clarification_consumed", result.ClarificationConsumed,
			)
		}
	}
}

func (m *Manager) startOutbound(cfg provider.ConnectionConfig, ready *atomic.Bool) {
	if m.cfg.Bus == nil || m.cfg.Queries == nil || m.cfg.Pool == nil || m.cfg.Registry == nil {
		return
	}
	notificationStore := outbound.NewDBNotificationStore(m.cfg.Pool)
	sub := outbound.NewSubscriber(
		m.cfg.Bus,
		newRegistryChannel(m.cfg.Registry, cfg.Provider, cfg.ConnectionID),
		outbound.NewDBBindingStore(m.cfg.Pool),
		outbound.NewDBPrefStore(m.cfg.Queries),
		"",
	)
	sub.SetFailureRecorder(m.cfg.Queries)
	if m.cfg.NotificationOutbox {
		sub.SetNotificationEnqueuer(notificationStore)
	}
	sub.SetActiveFunc(ready.Load)
	sub.Start()
	m.subscribers = append(m.subscribers, sub)
	slog.Info("channel manager: outbound subscriber started", "provider", cfg.Provider, "connection_id", cfg.ConnectionID)
}

func (m *Manager) startOutbox(ctx context.Context) {
	if m.cfg.Pool == nil || m.cfg.Queries == nil || m.cfg.Registry == nil {
		return
	}
	if m.cfg.NotificationOutbox {
		outboxCtx, cancel := context.WithCancel(ctx)
		m.cancels = append(m.cancels, cancel)
		worker := outbound.NewOutboxWorker(outbound.NewDBNotificationStore(m.cfg.Pool), newRegistryRetrySender(m.cfg.Registry))
		worker.SetActiveFunc(m.anyProviderReady)
		go worker.Run(outboxCtx)
	}
	if m.cfg.RetryWorkerEnabled {
		retryCtx, cancel := context.WithCancel(ctx)
		m.cancels = append(m.cancels, cancel)
		worker := outbound.NewRetryWorker(m.cfg.Pool, m.cfg.Queries, newRegistryRetrySender(m.cfg.Registry))
		worker.SetActiveFunc(m.anyProviderReady)
		go worker.Run(retryCtx)
	}
	if m.cfg.OutboundCleanupEnabled {
		cleanupCtx, cancel := context.WithCancel(ctx)
		m.cancels = append(m.cancels, cancel)
		worker := outbound.NewCleanupWorker(m.cfg.Queries)
		go worker.Run(cleanupCtx)
	}
}

func (m *Manager) anyProviderReady() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ready := range m.ready {
		if ready != nil && ready.Load() {
			return true
		}
	}
	return false
}

func waitReconnect(ctx context.Context, delay time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}

type connectionChannel struct {
	provider     string
	connectionID string
	base         port.Channel
}

func newConnectionChannel(connectionID string, base port.Channel) *connectionChannel {
	return &connectionChannel{provider: base.Name(), connectionID: connectionID, base: base}
}

func (c *connectionChannel) Name() string { return c.connectionID }

func (c *connectionChannel) ProviderName() string { return c.provider }

func (c *connectionChannel) ConnectionID() string { return c.connectionID }

func (c *connectionChannel) Connect(ctx context.Context) error { return c.base.Connect(ctx) }

func (c *connectionChannel) Disconnect(ctx context.Context) error { return c.base.Disconnect(ctx) }

func (c *connectionChannel) Send(ctx context.Context, msg port.OutboundMessage) (port.SendResult, error) {
	return c.base.Send(ctx, msg)
}

func (c *connectionChannel) SendCard(ctx context.Context, msg port.OutboundCardMessage) (port.SendResult, error) {
	return c.base.SendCard(ctx, msg)
}

func (c *connectionChannel) Events() <-chan port.InboundEvent { return c.base.Events() }

func (c *connectionChannel) GetChatInfo(ctx context.Context, chatID string) (port.ChatInfo, error) {
	return c.base.GetChatInfo(ctx, chatID)
}

func (c *connectionChannel) GetUserInfo(ctx context.Context, userID string) (port.UserInfo, error) {
	return c.base.GetUserInfo(ctx, userID)
}

type registryChannel struct {
	registry     *channel.Registry
	provider     string
	connectionID string
}

func newRegistryChannel(registry *channel.Registry, providerName, connectionID string) *registryChannel {
	return &registryChannel{registry: registry, provider: providerName, connectionID: connectionID}
}

func (c *registryChannel) Name() string { return c.connectionID }

func (c *registryChannel) ProviderName() string { return c.provider }

func (c *registryChannel) ConnectionID() string { return c.connectionID }

func (c *registryChannel) Connect(context.Context) error { return nil }

func (c *registryChannel) Disconnect(context.Context) error { return nil }

func (c *registryChannel) Events() <-chan port.InboundEvent { return nil }

func (c *registryChannel) Send(ctx context.Context, msg port.OutboundMessage) (port.SendResult, error) {
	ch, err := c.registry.Get(c.connectionID)
	if err != nil {
		return port.SendResult{Retryable: true}, fmt.Errorf("registry channel: get %s: %w", c.connectionID, err)
	}
	return ch.Send(ctx, msg)
}

func (c *registryChannel) SendCard(ctx context.Context, msg port.OutboundCardMessage) (port.SendResult, error) {
	ch, err := c.registry.Get(c.connectionID)
	if err != nil {
		return port.SendResult{Retryable: true}, fmt.Errorf("registry channel: get %s: %w", c.connectionID, err)
	}
	return ch.SendCard(ctx, msg)
}

func (c *registryChannel) GetChatInfo(ctx context.Context, chatID string) (port.ChatInfo, error) {
	ch, err := c.registry.Get(c.connectionID)
	if err != nil {
		return port.ChatInfo{}, fmt.Errorf("registry channel: get %s: %w", c.connectionID, err)
	}
	return ch.GetChatInfo(ctx, chatID)
}

func (c *registryChannel) GetUserInfo(ctx context.Context, userID string) (port.UserInfo, error) {
	ch, err := c.registry.Get(c.connectionID)
	if err != nil {
		return port.UserInfo{}, fmt.Errorf("registry channel: get %s: %w", c.connectionID, err)
	}
	return ch.GetUserInfo(ctx, userID)
}

type registryRetrySender struct {
	registry *channel.Registry
}

func newRegistryRetrySender(registry *channel.Registry) *registryRetrySender {
	return &registryRetrySender{registry: registry}
}

func (s *registryRetrySender) SendCard(ctx context.Context, connectionID string, externalUserID string, payload outbound.RetryPayload) error {
	ch, err := s.registry.Get(connectionID)
	if err != nil {
		return outbound.WrapRetryable(fmt.Errorf("retry sender: get %s: %w", connectionID, err))
	}
	result, err := ch.SendCard(ctx, port.OutboundCardMessage{
		Target: port.TargetUser(externalUserID),
		ChatID: externalUserID,
		Title:  payload.Title,
		Body:   payload.Body,
	})
	if err != nil && result.Retryable {
		return outbound.WrapRetryable(err)
	}
	return err
}

var (
	_ port.Channel         = (*registryChannel)(nil)
	_ port.Channel         = (*connectionChannel)(nil)
	_ outbound.RetrySender = (*registryRetrySender)(nil)
)
