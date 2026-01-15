-- +goose Up

-- Update unique constraint to include filter_id
-- This allows multiple filters to store the same blockchain event (same tx_hash, tx_lt, msg_index)
-- Query-time deduplication handles returning unique events to callers
DROP INDEX IF EXISTS ton.idx_logs_unique;
CREATE UNIQUE INDEX IF NOT EXISTS idx_logs_unique ON ton.log_poller_logs (chain_id, filter_id, tx_hash, tx_lt, msg_index);

-- Add index for GetLatestMasterBlockSeqno query optimization
-- Optimizes: SELECT MAX(master_block_seqno) FROM ton.log_poller_logs WHERE chain_id = ?
CREATE INDEX IF NOT EXISTS idx_logs_master_block ON ton.log_poller_logs (chain_id, master_block_seqno DESC);

-- +goose Down
DROP INDEX IF EXISTS ton.idx_logs_master_block;
DROP INDEX IF EXISTS ton.idx_logs_unique;
CREATE UNIQUE INDEX IF NOT EXISTS idx_logs_unique ON ton.log_poller_logs (tx_hash, tx_lt, msg_index);
