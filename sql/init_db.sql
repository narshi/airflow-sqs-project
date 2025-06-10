-- Filing tracking table
CREATE TABLE IF NOT EXISTS filing_tracker (
    id SERIAL PRIMARY KEY,
    filing_id VARCHAR(255) UNIQUE NOT NULL,
    status VARCHAR(50) DEFAULT 'PENDING',
    dag_run_id VARCHAR(255),
    filing_data JSONB,
    result_data JSONB,
    callback_url VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

-- Filing status history
CREATE TABLE IF NOT EXISTS filing_status_history (
    id SERIAL PRIMARY KEY,
    filing_id VARCHAR(255) REFERENCES filing_tracker(filing_id),
    status VARCHAR(50),
    message TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_filing_tracker_status ON filing_tracker(status);
CREATE INDEX IF NOT EXISTS idx_filing_tracker_filing_id ON filing_tracker(filing_id);
CREATE INDEX IF NOT EXISTS idx_filing_history_filing_id ON filing_status_history(filing_id);

-- Function to update timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to auto-update timestamp
CREATE TRIGGER update_filing_tracker_updated_at 
    BEFORE UPDATE ON filing_tracker 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();