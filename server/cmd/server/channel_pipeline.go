package main

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/multica-ai/multica/server/internal/channel"
	"github.com/multica-ai/multica/server/internal/channel/binding"
	"github.com/multica-ai/multica/server/internal/channel/facade"
	"github.com/multica-ai/multica/server/internal/channel/facadeimpl"
	"github.com/multica-ai/multica/server/internal/channel/inbound"
	"github.com/multica-ai/multica/server/internal/storage"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type channelPipelineOptions struct {
	Storage        storage.Storage
	FileDownloader inbound.FileDownloader
	Observer       inbound.Observer
}

func newChannelInboundPipeline(pool *pgxpool.Pool, registry *channel.Registry, opts ...channelPipelineOptions) *inbound.Pipeline {
	queries := db.New(pool)
	issueSvc := facadeimpl.NewIssueService(pool)
	commentSvc := facadeimpl.NewCommentService(queries, issueSvc)
	bindings := inbound.NewDBChatBindingLookup(pool)
	userResolver := inbound.NewDBUserInfoResolver(pool)
	issuer := binding.NewTokenIssuer(queries)

	steps := []inbound.Step{
		inbound.NewNormalizeStep(),
		inbound.NewDedupStep(inbound.NewDBDedupStore(queries)),
		inbound.NewUserIdentityBindStep(pool, registry, issuer),
		inbound.NewChatBindCommandStep(registry, issuer),
		inbound.NewSlashStep(inbound.SlashConfig{Registry: registry}),
		inbound.NewRuleIntentStep(),
		inbound.NewAuthzStep(inbound.AuthzConfig{
			Store:        bindings,
			Registry:     registry,
			SendReplies:  true,
			RejectAsSkip: true,
		}),
	}

	var opt channelPipelineOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	if opt.Storage != nil && opt.FileDownloader != nil {
		steps = append(steps, inbound.NewAttachmentStep(inbound.AttachmentConfig{
			Storage:           opt.Storage,
			AttachmentQuerier: facade.NewAttachmentFacade(facadeimpl.NewAttachmentService(queries)),
			FileDownloader:    opt.FileDownloader,
			Registry:          registry,
			ChatBinding:       bindings,
			UserResolver:      userResolver,
			IssueFacade:       facade.NewIssueFacade(issueSvc),
		}))
	} else if len(opts) > 0 && (opt.Storage != nil || opt.FileDownloader != nil) {
		slog.Info("channel attachment step disabled: storage or file downloader is not configured")
	}

	steps = append(steps,
		inbound.NewDispatchStep(inbound.DispatchConfig{
			IssueFacade:   facade.NewIssueFacade(issueSvc),
			CommentFacade: facade.NewCommentFacade(commentSvc),
			Registry:      registry,
			ChatBinding:   bindings,
			UserResolver:  userResolver,
		}),
		inbound.NewReplyStep(),
	)

	pipeline := inbound.NewPipeline(steps...)
	pipeline.SetObserver(opt.Observer)
	return pipeline
}
