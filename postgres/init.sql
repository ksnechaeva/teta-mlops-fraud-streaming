CREATE TABLE IF NOT EXISTS transaction_scores (
    transaction_id TEXT PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL CHECK (score >= 0 AND score <= 1),
    fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0, 1)),
    scored_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transaction_scores_scored_at
    ON transaction_scores (scored_at DESC);

CREATE INDEX IF NOT EXISTS idx_transaction_scores_fraud
    ON transaction_scores (fraud_flag, scored_at DESC);
