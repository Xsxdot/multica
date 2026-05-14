-- No-op: migration 075 is an idempotent compatibility migration for
-- databases that already ran 070 before token purpose/chat metadata existed.
-- The canonical schema now lives in 070, so rolling back 075 must not drop
-- columns or constraints that a fresh database receives from 070.
SELECT 1;
