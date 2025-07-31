# Schema Evolution Changelog

This document tracks changes to the Avro schemas and database schema over time.

## Avro Schema Versions

### v2 (Current) - May 2025

#### Added Fields:
- `processing_timestamp` (Union[null, double]) - Timestamp when the message is processed by the Rust trading engine
  - Used for latency calculations and performance monitoring
  - Default: null (for backward compatibility)

#### Affected Schemas:
- ticker/v2.avsc
- trade/v2.avsc
- ack/v2.avsc

### v1 (Initial) - April 2025

- Initial schema definitions for all message types
- Defined basic fields for ticker, trade, and ack messages

## Database Schema Migrations

### 002_add_processing_timestamp.sql - May 2025

#### Added Columns:
- `processing_timestamp` (DOUBLE PRECISION) - When the message was processed by Rust engine
- `rust_to_db_latency_ms` (DOUBLE PRECISION) - Latency between Rust processing and database ingestion
- `exchange_to_rust_latency_ms` (DOUBLE PRECISION) - Latency between exchange timestamp and Rust processing
- `total_latency_ms` (DOUBLE PRECISION) - Total end-to-end latency

#### Added Functionality:
- Database triggers for automatic latency calculations on insert/update
- Functions to calculate latency metrics based on timestamps
- Update existing rows with latency calculations

#### Affected Tables:
- ticker_data
- trade_data
- ack_data

### 001_initial_schema.sql - April 2025

- Initial database schema setup
- Created base tables for storing message data
- Added migrations tracking table

## Compatibility Notes

- All schema changes maintain backward compatibility through union types and default values
- Applications using v1 schemas can still read data produced with v2 schemas
- Applications using v2 schemas can read data produced with v1 schemas (missing fields will be null)
- Database changes are non-breaking, with new columns having nullable values
