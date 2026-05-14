CREATE TABLE channel_user_binding (
    id                 UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    provider           TEXT         NOT NULL CHECK (provider IN ('feishu')),
    external_user_id   TEXT         NOT NULL,
    user_id            UUID         NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
    external_name      TEXT,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (provider, external_user_id),
    UNIQUE (provider, user_id)
);
