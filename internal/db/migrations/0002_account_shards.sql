-- Sharded system accounts.
--
-- A hot account (processor_clearing:CUR, fee_revenue:CUR) is split into
-- shard accounts "<base>", "<base>#1", "<base>#2", ... Each shard is an
-- ordinary account row with its own cached balance, so postings to different
-- shards do not contend on one row lock. The logical account's balance is the
-- sum of its shards' balances.
--
-- Nothing else changes: the postings_apply trigger still maintains each
-- shard's cached balance, entries still balance, postings are still
-- append-only. Shard "" (the unsuffixed code) is the account that existed
-- before this migration, so existing data needs no rewrite.
--
-- base_code groups the shards of one logical account. Ordinary accounts are
-- their own group of one.
ALTER TABLE accounts
    ADD COLUMN base_code text GENERATED ALWAYS AS (split_part(code, '#', 1)) STORED;
CREATE INDEX accounts_base_code_idx ON accounts (base_code);
