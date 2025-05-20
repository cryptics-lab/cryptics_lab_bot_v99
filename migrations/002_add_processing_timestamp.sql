-- Migration for adding processing_timestamp fields to track latency

-- Add processing_timestamp column to ticker_data table
ALTER TABLE public.ticker_data ADD COLUMN IF NOT EXISTS processing_timestamp DOUBLE PRECISION;

-- Add processing_timestamp column to trade_data table
ALTER TABLE public.trade_data ADD COLUMN IF NOT EXISTS processing_timestamp DOUBLE PRECISION;

-- Add processing_timestamp column to ack_data table
ALTER TABLE public.ack_data ADD COLUMN IF NOT EXISTS processing_timestamp DOUBLE PRECISION;

-- Add latency columns (not generated, will use triggers instead)
-- Rust processing to DB ingestion latency (in milliseconds)
ALTER TABLE public.ticker_data ADD COLUMN IF NOT EXISTS rust_to_db_latency_ms DOUBLE PRECISION;
ALTER TABLE public.trade_data ADD COLUMN IF NOT EXISTS rust_to_db_latency_ms DOUBLE PRECISION;
ALTER TABLE public.ack_data ADD COLUMN IF NOT EXISTS rust_to_db_latency_ms DOUBLE PRECISION;

-- Exchange to Rust processing latency (in milliseconds)
ALTER TABLE public.ticker_data ADD COLUMN IF NOT EXISTS exchange_to_rust_latency_ms DOUBLE PRECISION;
ALTER TABLE public.trade_data ADD COLUMN IF NOT EXISTS exchange_to_rust_latency_ms DOUBLE PRECISION;
ALTER TABLE public.ack_data ADD COLUMN IF NOT EXISTS exchange_to_rust_latency_ms DOUBLE PRECISION;

-- Total end-to-end latency (in milliseconds)
ALTER TABLE public.ticker_data ADD COLUMN IF NOT EXISTS total_latency_ms DOUBLE PRECISION;
ALTER TABLE public.trade_data ADD COLUMN IF NOT EXISTS total_latency_ms DOUBLE PRECISION;
ALTER TABLE public.ack_data ADD COLUMN IF NOT EXISTS total_latency_ms DOUBLE PRECISION;

-- Create functions for calculating latency
CREATE OR REPLACE FUNCTION calculate_ticker_latency()
RETURNS TRIGGER AS $$
BEGIN
    -- Rust to DB latency
    IF NEW.processing_timestamp IS NOT NULL THEN
        NEW.rust_to_db_latency_ms := (EXTRACT(EPOCH FROM NEW.time_ts) * 1000) - (NEW.processing_timestamp * 1000);
    END IF;
    
    -- Exchange to Rust latency
    IF NEW.processing_timestamp IS NOT NULL AND NEW.mark_timestamp IS NOT NULL THEN
        NEW.exchange_to_rust_latency_ms := (NEW.processing_timestamp * 1000) - (NEW.mark_timestamp * 1000);
    END IF;
    
    -- Total latency
    IF NEW.mark_timestamp IS NOT NULL THEN
        NEW.total_latency_ms := (EXTRACT(EPOCH FROM NEW.time_ts) * 1000) - (NEW.mark_timestamp * 1000);
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION calculate_trade_latency()
RETURNS TRIGGER AS $$
BEGIN
    -- Rust to DB latency
    IF NEW.processing_timestamp IS NOT NULL THEN
        NEW.rust_to_db_latency_ms := (EXTRACT(EPOCH FROM NEW.time_ts) * 1000) - (NEW.processing_timestamp * 1000);
    END IF;
    
    -- Exchange to Rust latency
    IF NEW.processing_timestamp IS NOT NULL AND NEW.time IS NOT NULL THEN
        NEW.exchange_to_rust_latency_ms := (NEW.processing_timestamp * 1000) - (NEW.time * 1000);
    END IF;
    
    -- Total latency
    IF NEW.time IS NOT NULL THEN
        NEW.total_latency_ms := (EXTRACT(EPOCH FROM NEW.time_ts) * 1000) - (NEW.time * 1000);
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION calculate_ack_latency()
RETURNS TRIGGER AS $$
BEGIN
    -- Rust to DB latency
    IF NEW.processing_timestamp IS NOT NULL THEN
        NEW.rust_to_db_latency_ms := (EXTRACT(EPOCH FROM NEW.time_ts) * 1000) - (NEW.processing_timestamp * 1000);
    END IF;
    
    -- Exchange to Rust latency
    IF NEW.processing_timestamp IS NOT NULL AND NEW.create_time IS NOT NULL THEN
        NEW.exchange_to_rust_latency_ms := (NEW.processing_timestamp * 1000) - (NEW.create_time * 1000);
    END IF;
    
    -- Total latency
    IF NEW.create_time IS NOT NULL THEN
        NEW.total_latency_ms := (EXTRACT(EPOCH FROM NEW.time_ts) * 1000) - (NEW.create_time * 1000);
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers for real-time latency calculation
DROP TRIGGER IF EXISTS ticker_latency_trigger ON public.ticker_data;
CREATE TRIGGER ticker_latency_trigger
BEFORE INSERT OR UPDATE ON public.ticker_data
FOR EACH ROW EXECUTE FUNCTION calculate_ticker_latency();

DROP TRIGGER IF EXISTS trade_latency_trigger ON public.trade_data;
CREATE TRIGGER trade_latency_trigger
BEFORE INSERT OR UPDATE ON public.trade_data
FOR EACH ROW EXECUTE FUNCTION calculate_trade_latency();

DROP TRIGGER IF EXISTS ack_latency_trigger ON public.ack_data;
CREATE TRIGGER ack_latency_trigger
BEFORE INSERT OR UPDATE ON public.ack_data
FOR EACH ROW EXECUTE FUNCTION calculate_ack_latency();

-- Update existing rows to calculate latency
UPDATE public.ticker_data
SET 
    rust_to_db_latency_ms = CASE 
        WHEN processing_timestamp IS NOT NULL THEN (EXTRACT(EPOCH FROM time_ts) * 1000) - (processing_timestamp * 1000)
        ELSE NULL 
    END,
    exchange_to_rust_latency_ms = CASE 
        WHEN processing_timestamp IS NOT NULL AND mark_timestamp IS NOT NULL THEN (processing_timestamp * 1000) - (mark_timestamp * 1000)
        ELSE NULL 
    END,
    total_latency_ms = CASE 
        WHEN mark_timestamp IS NOT NULL THEN (EXTRACT(EPOCH FROM time_ts) * 1000) - (mark_timestamp * 1000)
        ELSE NULL 
    END
WHERE true;

UPDATE public.trade_data
SET 
    rust_to_db_latency_ms = CASE 
        WHEN processing_timestamp IS NOT NULL THEN (EXTRACT(EPOCH FROM time_ts) * 1000) - (processing_timestamp * 1000)
        ELSE NULL 
    END,
    exchange_to_rust_latency_ms = CASE 
        WHEN processing_timestamp IS NOT NULL AND time IS NOT NULL THEN (processing_timestamp * 1000) - (time * 1000)
        ELSE NULL 
    END,
    total_latency_ms = CASE 
        WHEN time IS NOT NULL THEN (EXTRACT(EPOCH FROM time_ts) * 1000) - (time * 1000)
        ELSE NULL 
    END
WHERE true;

UPDATE public.ack_data
SET 
    rust_to_db_latency_ms = CASE 
        WHEN processing_timestamp IS NOT NULL THEN (EXTRACT(EPOCH FROM time_ts) * 1000) - (processing_timestamp * 1000)
        ELSE NULL 
    END,
    exchange_to_rust_latency_ms = CASE 
        WHEN processing_timestamp IS NOT NULL AND create_time IS NOT NULL THEN (processing_timestamp * 1000) - (create_time * 1000)
        ELSE NULL 
    END,
    total_latency_ms = CASE 
        WHEN create_time IS NOT NULL THEN (EXTRACT(EPOCH FROM time_ts) * 1000) - (create_time * 1000)
        ELSE NULL 
    END
WHERE true;
