-- Initialize PostgreSQL database for CrypticsLabBot
-- This file sets up PostgreSQL tables with UUID primary keys

-- PostgreSQL doesn't support IF NOT EXISTS with CREATE DATABASE
-- Use this syntax instead to avoid errors
SELECT 'CREATE DATABASE cryptics'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'cryptics');

\c cryptics;

-- Create extension for UUID support
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =====================
-- TICKER DATA TABLE
-- =====================
CREATE TABLE IF NOT EXISTS public.ticker_data (
    id UUID NOT NULL DEFAULT uuid_generate_v4(),
    instrument_name VARCHAR(100) NOT NULL,
    mark_price DOUBLE PRECISION NOT NULL,
    mark_timestamp DOUBLE PRECISION NOT NULL,
    best_bid_price DOUBLE PRECISION NOT NULL,
    best_bid_amount DOUBLE PRECISION NOT NULL,
    best_ask_price DOUBLE PRECISION NOT NULL,
    best_ask_amount DOUBLE PRECISION NOT NULL,
    last_price DOUBLE PRECISION NOT NULL,
    delta DOUBLE PRECISION NOT NULL,
    volume_24h DOUBLE PRECISION NOT NULL,
    value_24h DOUBLE PRECISION NOT NULL,
    low_price_24h DOUBLE PRECISION NOT NULL,
    high_price_24h DOUBLE PRECISION NOT NULL,
    change_24h DOUBLE PRECISION NOT NULL,
    index_price DOUBLE PRECISION NOT NULL,
    forward DOUBLE PRECISION NOT NULL,
    funding_mark DOUBLE PRECISION NOT NULL,
    funding_rate DOUBLE PRECISION NOT NULL,
    collar_low DOUBLE PRECISION NOT NULL,
    collar_high DOUBLE PRECISION NOT NULL,
    realised_funding_24h DOUBLE PRECISION NOT NULL,
    average_funding_rate_24h DOUBLE PRECISION NOT NULL,
    open_interest DOUBLE PRECISION NOT NULL,
    time_ts TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

-- Create a trigger to populate time_ts from mark_timestamp
CREATE OR REPLACE FUNCTION ticker_update_time_ts()
RETURNS TRIGGER AS $$
BEGIN
    -- Convert mark_timestamp (seconds since epoch) to timestamptz
    NEW.time_ts = to_timestamp(NEW.mark_timestamp);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trigger_ticker_time_ts
BEFORE INSERT OR UPDATE ON public.ticker_data
FOR EACH ROW
EXECUTE FUNCTION ticker_update_time_ts();

-- Create indexes for ticker queries
CREATE INDEX IF NOT EXISTS idx_ticker_timestamp ON public.ticker_data (mark_timestamp);
CREATE INDEX IF NOT EXISTS idx_ticker_name ON public.ticker_data (instrument_name);
CREATE INDEX IF NOT EXISTS idx_ticker_time_ts ON public.ticker_data (time_ts DESC);

-- Create a unique constraint for the connector's upsert operations
ALTER TABLE public.ticker_data ADD CONSTRAINT ticker_data_id_key UNIQUE (id);

-- =====================
-- ACK DATA TABLE (NEW)
-- =====================
CREATE TABLE IF NOT EXISTS public.ack_data (
    id UUID NOT NULL DEFAULT uuid_generate_v4(),
    order_id VARCHAR(100) NOT NULL,
    client_order_id BIGINT,
    instrument_name VARCHAR(100) NOT NULL,
    direction VARCHAR(50) NOT NULL,
    price DOUBLE PRECISION,
    amount DOUBLE PRECISION NOT NULL,
    filled_amount DOUBLE PRECISION NOT NULL,
    remaining_amount DOUBLE PRECISION NOT NULL,
    status VARCHAR(50) NOT NULL,
    order_type VARCHAR(50) NOT NULL,
    time_in_force VARCHAR(50) NOT NULL,
    change_reason VARCHAR(50) NOT NULL,
    delete_reason VARCHAR(50),
    insert_reason VARCHAR(50),
    create_time DOUBLE PRECISION NOT NULL,
    persistent BOOLEAN NOT NULL,
    time_ts TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

-- Create a trigger to populate time_ts from create_time
CREATE OR REPLACE FUNCTION ack_update_time_ts()
RETURNS TRIGGER AS $$
BEGIN
    -- Convert create_time (seconds since epoch) to timestamptz
    NEW.time_ts = to_timestamp(NEW.create_time);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trigger_ack_time_ts
BEFORE INSERT OR UPDATE ON public.ack_data
FOR EACH ROW
EXECUTE FUNCTION ack_update_time_ts();

-- Create indexes for ack queries
CREATE INDEX IF NOT EXISTS idx_ack_order_id ON public.ack_data (order_id);
CREATE INDEX IF NOT EXISTS idx_ack_client_order_id ON public.ack_data (client_order_id);
CREATE INDEX IF NOT EXISTS idx_ack_instrument ON public.ack_data (instrument_name);
CREATE INDEX IF NOT EXISTS idx_ack_status ON public.ack_data (status);
CREATE INDEX IF NOT EXISTS idx_ack_create_time ON public.ack_data (create_time);
CREATE INDEX IF NOT EXISTS idx_ack_time_ts ON public.ack_data (time_ts DESC);

-- Create a unique constraint for the connector's upsert operations
ALTER TABLE public.ack_data ADD CONSTRAINT ack_data_id_key UNIQUE (id);

-- =====================
-- TRADE DATA TABLE (NEW)
-- =====================
CREATE TABLE IF NOT EXISTS public.trade_data (
    id UUID NOT NULL DEFAULT uuid_generate_v4(),
    trade_id VARCHAR(100) NOT NULL,
    order_id VARCHAR(100) NOT NULL,
    client_order_id BIGINT,
    instrument_name VARCHAR(100) NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    maker_taker VARCHAR(50) NOT NULL,
    time DOUBLE PRECISION NOT NULL,
    time_ts TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

-- Create a trigger to populate time_ts from time
CREATE OR REPLACE FUNCTION trade_update_time_ts()
RETURNS TRIGGER AS $$
BEGIN
    -- Convert time (seconds since epoch) to timestamptz
    NEW.time_ts = to_timestamp(NEW.time);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trigger_trade_time_ts
BEFORE INSERT OR UPDATE ON public.trade_data
FOR EACH ROW
EXECUTE FUNCTION trade_update_time_ts();

-- Create indexes for trade queries
CREATE INDEX IF NOT EXISTS idx_trade_trade_id ON public.trade_data (trade_id);
CREATE INDEX IF NOT EXISTS idx_trade_order_id ON public.trade_data (order_id);
CREATE INDEX IF NOT EXISTS idx_trade_client_order_id ON public.trade_data (client_order_id);
CREATE INDEX IF NOT EXISTS idx_trade_instrument ON public.trade_data (instrument_name);
CREATE INDEX IF NOT EXISTS idx_trade_time ON public.trade_data (time);
CREATE INDEX IF NOT EXISTS idx_trade_time_ts ON public.trade_data (time_ts DESC);

-- Create a unique constraint for the connector's upsert operations
ALTER TABLE public.trade_data ADD CONSTRAINT trade_data_trade_id_key UNIQUE (trade_id);

-- =====================
-- ORDER TABLE (NEW)
-- =====================
CREATE TABLE IF NOT EXISTS public.orders (
    id UUID NOT NULL DEFAULT uuid_generate_v4(),
    order_id VARCHAR(100) NOT NULL,
    client_order_id BIGINT,
    instrument_name VARCHAR(100) NOT NULL,
    direction VARCHAR(50) NOT NULL,
    price DOUBLE PRECISION,
    amount DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

-- Create indexes for order queries
CREATE INDEX IF NOT EXISTS idx_orders_order_id ON public.orders (order_id);
CREATE INDEX IF NOT EXISTS idx_orders_client_order_id ON public.orders (client_order_id);
CREATE INDEX IF NOT EXISTS idx_orders_instrument ON public.orders (instrument_name);

-- Create a unique constraint for the order_id
ALTER TABLE public.orders ADD CONSTRAINT orders_order_id_key UNIQUE (order_id);

-- =====================
-- ORDER STATUS TABLE (NEW)
-- =====================
CREATE TABLE IF NOT EXISTS public.order_status (
    id UUID NOT NULL DEFAULT uuid_generate_v4(),
    order_id VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    filled_amount DOUBLE PRECISION NOT NULL,
    remaining_amount DOUBLE PRECISION NOT NULL,
    change_reason VARCHAR(50) NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    FOREIGN KEY (order_id) REFERENCES public.orders(order_id)
);

-- Create indexes for order status queries
CREATE INDEX IF NOT EXISTS idx_order_status_order_id ON public.order_status (order_id);
CREATE INDEX IF NOT EXISTS idx_order_status_status ON public.order_status (status);
CREATE INDEX IF NOT EXISTS idx_order_status_changed_at ON public.order_status (changed_at DESC);

-- =====================
-- INDEX DATA TABLE
-- =====================
CREATE TABLE IF NOT EXISTS public.index_data (
    id UUID NOT NULL DEFAULT uuid_generate_v4(),
    index_name VARCHAR(100) NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,
    previous_settlement_price DOUBLE PRECISION NOT NULL,
    time_ts TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

-- Create a trigger to populate time_ts from timestamp
CREATE OR REPLACE FUNCTION index_update_time_ts()
RETURNS TRIGGER AS $$
BEGIN
    -- Convert timestamp (seconds since epoch) to timestamptz
    NEW.time_ts = to_timestamp(NEW.timestamp);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trigger_index_time_ts
BEFORE INSERT OR UPDATE ON public.index_data
FOR EACH ROW
EXECUTE FUNCTION index_update_time_ts();

-- Create indexes for index queries
CREATE INDEX IF NOT EXISTS idx_index_name ON public.index_data (index_name);
CREATE INDEX IF NOT EXISTS idx_index_timestamp ON public.index_data (timestamp);
CREATE INDEX IF NOT EXISTS idx_index_time_ts ON public.index_data (time_ts DESC);

-- Create a unique constraint for the connector's upsert operations
ALTER TABLE public.index_data ADD CONSTRAINT index_data_id_key UNIQUE (id);

-- Create stored procedure to update order and order_status tables from ack
CREATE OR REPLACE FUNCTION process_ack()
RETURNS TRIGGER AS $$
BEGIN
    -- Update or insert into orders table
    INSERT INTO public.orders (
        order_id, 
        client_order_id, 
        instrument_name, 
        direction, 
        price, 
        amount
    ) VALUES (
        NEW.order_id,
        NEW.client_order_id,
        NEW.instrument_name,
        NEW.direction,
        NEW.price,
        NEW.amount
    )
    ON CONFLICT (order_id) DO UPDATE SET
        client_order_id = EXCLUDED.client_order_id,
        price = EXCLUDED.price,
        amount = EXCLUDED.amount,
        updated_at = CURRENT_TIMESTAMP;
        
    -- Insert into order_status table
    INSERT INTO public.order_status (
        order_id,
        status,
        filled_amount,
        remaining_amount,
        change_reason
    ) VALUES (
        NEW.order_id,
        NEW.status,
        NEW.filled_amount,
        NEW.remaining_amount,
        NEW.change_reason
    );
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for ack processing
CREATE OR REPLACE TRIGGER trigger_process_ack
AFTER INSERT ON public.ack_data
FOR EACH ROW
EXECUTE FUNCTION process_ack();

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE cryptics TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
