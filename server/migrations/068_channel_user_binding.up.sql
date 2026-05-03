-- Bidirectional unique mapping between an external IM provider's user (e.g.
-- a Feishu open_id) and a Multica user. Enforces PRD AC3.2/AC3.3:
--   - one external user can only bind to one Multica user
--   - one Multica user can only bind to one external user per provider
-- Multi-provider per Multica user is allowed (a user may bind both Feishu
-- and Slack in future).
CREATE TABLE channel_user_binding (
    id                 UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    provider           TEXT         NOT NULL CHECK (provider IN ('feishu')),
    external_user_id   TEXT         NOT NULL,
    user_id            UUID         NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
    external_name      TEXT,            -- snapshot of platform display name at bind time
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (provider, external_user_id),  -- AC3.2: external->multica side
    UNIQUE (provider, user_id)            -- AC3.3: multica->external side
);

-- Hot path: inbound pipeline looks up "is this Feishu open_id bound?"
-- on every @Bot message. Covered by the (provider, external_user_id) unique
-- index above -- no extra index needed.

-- Hot path: outbound subscriber looks up "what's the Feishu open_id for this
-- Multica user?" Covered by (provider, user_id) unique index above.
