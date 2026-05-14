ALTER TABLE channel_inbound_event
    DROP CONSTRAINT IF EXISTS channel_inbound_event_wait_kind_check,
    ADD CONSTRAINT channel_inbound_event_wait_kind_check
        CHECK (wait_kind IN ('intent', 'action', 'user_clarification'));
