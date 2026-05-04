package outbound

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/channel/port"
	"github.com/multica-ai/multica/server/internal/events"
)

var (
	ErrNotBound = errors.New("outbound: user not bound to channel")
)

type BindingStore interface {
	FindUserID(ctx context.Context, provider, externalUserID string) (pgtype.UUID, error)
	ResolveExternalID(ctx context.Context, provider, userID string) (string, error)
}

type DBPool interface {
	QueryRow(ctx context.Context, sql string, args ...any) Row
}

type Row interface {
	Scan(dest ...any) error
}

type DBBindingStore struct {
	pool DBPool
}

func NewDBBindingStore(pool DBPool) *DBBindingStore {
	return &DBBindingStore{pool: pool}
}

func (s *DBBindingStore) FindUserID(ctx context.Context, provider, externalUserID string) (pgtype.UUID, error) {
	return pgtype.UUID{}, nil
}

func (s *DBBindingStore) ResolveExternalID(ctx context.Context, provider, userID string) (string, error) {
	return "", nil
}

type Subscriber struct {
	bus         *events.Bus
	channel     port.Channel
	bindings    BindingStore
	prefs       PrefStore
	workspaceID string
}

func NewSubscriber(
	bus *events.Bus,
	ch port.Channel,
	bindings BindingStore,
	prefs PrefStore,
	workspaceID string,
) *Subscriber {
	return &Subscriber{
		bus:         bus,
		channel:     ch,
		bindings:    bindings,
		prefs:       prefs,
		workspaceID: workspaceID,
	}
}

func (s *Subscriber) Start() {}

func (s *Subscriber) Stop() {}

func extractStringSlice(v any) []string {
	return nil
}

func mapInboxTypeToEventKind(inboxType string) string {
	return ""
}

func parseUUID(s string) pgtype.UUID {
	return pgtype.UUID{}
}
