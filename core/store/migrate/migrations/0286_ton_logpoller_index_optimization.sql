-- +goose Up

-- Add error field for storing parse/validation errors (nullable TEXT for failed message processing)
ALTER TABLE ton.log_poller_logs ADD COLUMN IF NOT EXISTS error TEXT;

-- Update unique constraint to include filter_id
-- This allows multiple filters to store the same blockchain event (same tx_hash, tx_lt, msg_index)
-- Query-time deduplication handles returning unique events to callers
DROP INDEX IF EXISTS ton.idx_logs_unique;
CREATE UNIQUE INDEX IF NOT EXISTS idx_logs_unique ON ton.log_poller_logs (chain_id, filter_id, tx_hash, tx_lt, msg_index);

-- Update filters index to include chain_id for efficient multi-chain filtering
DROP INDEX IF EXISTS ton.idx_filters_address_msgtype;
CREATE INDEX IF NOT EXISTS idx_filters_address_msgtype ON ton.log_poller_filters(chain_id, address, msg_type);

-- Checkpoint resumption index: used on service restart to find last processed masterchain block
-- Optimizes: SELECT MAX(master_block_seqno) FROM ton.log_poller_logs WHERE chain_id = ?
CREATE INDEX IF NOT EXISTS idx_logs_master_block ON ton.log_poller_logs (chain_id, master_block_seqno DESC);

-- +goose Down
DROP INDEX IF EXISTS ton.idx_logs_master_block;
DROP INDEX IF EXISTS ton.idx_filters_address_msgtype;
CREATE INDEX IF NOT EXISTS idx_filters_address_msgtype ON ton.log_poller_filters(address, msg_type);
DROP INDEX IF EXISTS ton.idx_logs_unique;
CREATE UNIQUE INDEX IF NOT EXISTS idx_logs_unique ON ton.log_poller_logs (tx_hash, tx_lt, msg_index);
ALTER TABLE ton.log_poller_logs DROP COLUMN IF EXISTS error;
