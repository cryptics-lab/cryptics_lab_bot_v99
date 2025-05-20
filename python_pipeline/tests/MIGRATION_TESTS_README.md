# Migration System Tests

This directory contains tests for the schema migration system that adds latency tracking capabilities to the database.

## Test Files

- `test_schema_migration.py`: Comprehensive tests for the migration system that verifies:
  - Migrations are applied correctly
  - New columns are created properly
  - Latency calculations work as expected
  - The migration files contain the correct SQL statements

- `create_test_db.py`: A standalone script to create a test database with migrations applied, which can be used for testing without running the entire pipeline.

## Running the Tests

To run the migration tests, use pytest:

```bash
cd /Users/Borat/Desktop/code/cryptics_lab_bot_v99/python_pipeline
python -m pytest tests/test_schema_migration.py -v
```

## Creating a Test Database Without Running the Pipeline

You can create a dedicated test database with all migrations applied by running the create_test_db.py script:

```bash
cd /Users/Borat/Desktop/code/cryptics_lab_bot_v99/python_pipeline
./tests/create_test_db.py
```

This will:
1. Create a test database named `cryptics_migration_test`
2. Create the initial tables and schema
3. Apply all migrations including the latency tracking fields
4. Verify that the migrations were applied correctly

The script will output connection details that you can use to connect to the test database.

## Testing Through PostgreSQL

After creating the test database, you can connect to it directly using psql:

```bash
psql -h localhost -p 5433 -U postgres -d cryptics_migration_test
```

You can then verify the schema changes:

```sql
-- Check that the processing_timestamp column exists
\d ticker_data

-- Check that latency columns are calculated correctly
INSERT INTO ticker_data (
    instrument_name, mark_price, mark_timestamp, 
    best_bid_price, best_bid_amount, best_ask_price, best_ask_amount,
    processing_timestamp
) VALUES (
    'BTC-PERPETUAL', 50000.0, 
    extract(epoch from now()) - 0.2, -- 200ms ago
    49950.0, 1.0, 50050.0, 1.0,
    extract(epoch from now()) - 0.05 -- 50ms ago
);

-- Query the latency calculations
SELECT 
    instrument_name,
    mark_timestamp,
    processing_timestamp,
    EXTRACT(EPOCH FROM time_ts) as time_ts_epoch,
    exchange_to_rust_latency_ms,
    rust_to_db_latency_ms,
    total_latency_ms
FROM ticker_data
ORDER BY id DESC
LIMIT 1;
```

## Integration with CI/CD

The test scripts can be integrated into CI/CD pipelines to verify that schema migrations work correctly before deploying to production. Simply add the pytest command to your CI/CD configuration:

```yaml
- name: Test schema migrations
  run: python -m pytest python_pipeline/tests/test_schema_migration.py -v
```
