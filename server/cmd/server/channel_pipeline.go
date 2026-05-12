package main

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/multica-ai/multica/server/internal/channel/binding"
	"github.com/multica-ai/multica/server/internal/channel/facade"
	"github.com/multica-ai/multica/server/internal/channel/facadeimpl"
	"github.com/multica-ai/multica/server/internal/channel/inbound"
	chintent "github.com/multica-ai/multica/server/internal/channel/intent"
	"github.com/multica-ai/multica/server/internal/channel/port"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/storage"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type channelPipelineOptions struct {
	Storage         storage.Storage
	FileDownloader  port.FileDownloader
	Gateway         port.ChannelGateway
	Observer        inbound.Observer
	ChatIntent      chintent.ChatIntentClient
	AsyncChatIntent chintent.AsyncChatIntentClient
	TaskService     *service.TaskService
}

type channelInboundRuntimeComponents struct {
	PrePipeline   *inbound.Pipeline
	PostPipeline  *inbound.Pipeline
	RuleResolvers []chintent.IntentResolver
	ChatIntent    chintent.AsyncChatIntentClient
}

func newChannelInboundRuntimeComponents(pool *pgxpool.Pool, opts ...channelPipelineOptions) channelInboundRuntimeComponents {
	queries := db.New(pool)
	issueSvc := facadeimpl.NewIssueService(pool)
	commentSvc := facadeimpl.NewCommentService(queries, issueSvc)
	bindings := inbound.NewDBChatBindingLookup(pool)
	userResolver := inbound.NewDBUserInfoResolver(pool)
	issuer := binding.NewTokenIssuer(queries)

	var opt channelPipelineOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	ruleResolvers := []chintent.IntentResolver{
		chintent.NewRuleResolver(chintent.NewRuleMatcher()),
	}
	asyncChatIntent := opt.AsyncChatIntent
	if asyncChatIntent == nil {
		if typed, ok := opt.ChatIntent.(chintent.AsyncChatIntentClient); ok {
			asyncChatIntent = typed
		}
	}
	if asyncChatIntent == nil && opt.TaskService != nil {
		asyncChatIntent = facadeimpl.NewTaskBackedChatIntentClient(queries, opt.TaskService, bindings)
	}

	pre := inbound.NewPipeline(
		inbound.NewNormalizeStep(),
		inbound.NewUserIdentityBindStep(pool, opt.Gateway, issuer),
		inbound.NewChatBindCommandStep(opt.Gateway, issuer),
		inbound.NewSlashStep(inbound.SlashConfig{Gateway: opt.Gateway}),
	)
	pre.SetObserver(opt.Observer)

	postSteps := []inbound.Step{
		inbound.NewAuthzStep(inbound.AuthzConfig{
			Store:        bindings,
			Gateway:      opt.Gateway,
			SendReplies:  true,
			RejectAsSkip: true,
		}),
	}
	if opt.Storage != nil && (opt.FileDownloader != nil || opt.Gateway != nil) {
		postSteps = append(postSteps, inbound.NewAttachmentStep(inbound.AttachmentConfig{
			Storage:           opt.Storage,
			AttachmentQuerier: facade.NewAttachmentFacade(facadeimpl.NewAttachmentService(queries)),
			FileDownloader:    opt.FileDownloader,
			Gateway:           opt.Gateway,
			ChatBinding:       bindings,
			UserResolver:      userResolver,
			IssueFacade:       facade.NewIssueFacade(issueSvc),
		}))
	} else if len(opts) > 0 && (opt.Storage != nil || opt.FileDownloader != nil) {
		slog.Info("channel attachment step disabled: storage or file downloader is not configured")
	}
	postSteps = append(postSteps,
		inbound.NewDispatchStep(inbound.DispatchConfig{
			IssueFacade:      facade.NewIssueFacade(issueSvc),
			CommentFacade:    facade.NewCommentFacade(commentSvc),
			Gateway:          opt.Gateway,
			ChatBinding:      bindings,
			UserResolver:     userResolver,
			ProjectValidator: inbound.NewDBProjectWorkspaceValidator(pool),
			DispatchStore:    inbound.NewDBDispatchCompletionStore(pool),
		}),
		inbound.NewReplyStep(),
	)
	post := inbound.NewPipeline(postSteps...)
	post.SetObserver(opt.Observer)

	return channelInboundRuntimeComponents{
		PrePipeline:   pre,
		PostPipeline:  post,
		RuleResolvers: ruleResolvers,
		ChatIntent:    asyncChatIntent,
	}
}
