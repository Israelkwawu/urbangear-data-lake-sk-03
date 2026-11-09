-- SK-03 warehouse bootstrap. Runs ONCE, when the warehouse volume is first created.
-- (Changed this file? Re-run it with:  docker exec -i warehouse psql -U dwh_user -d warehouse < sql/warehouse_init.sql)

CREATE SCHEMA IF NOT EXISTS staging;     -- raw loads from the data lake (pipeline writes here)
CREATE SCHEMA IF NOT EXISTS analytics;   -- dbt marts (analysts read here)

-- Landing table for processed orders. One row per order line.
-- Columns = the Spark job's clean output (Lab 04 / guided lab Step 1.4).
CREATE TABLE IF NOT EXISTS staging.daily_orders (
    order_id            TEXT,
    customer_id         TEXT,
    platform            TEXT,
    order_date          TIMESTAMP,
    product_id          TEXT,
    product_name        TEXT,
    category            TEXT,
    quantity            INTEGER,
    unit_price          DOUBLE PRECISION,
    discount_pct        DOUBLE PRECISION,
    shipping_cost       DOUBLE PRECISION,
    tax_amount          DOUBLE PRECISION,
    payment_method      TEXT,
    shipping_country    TEXT,
    shipping_state      TEXT,
    shipping_city       TEXT,
    is_returned         BOOLEAN,
    gross_revenue       DOUBLE PRECISION,
    discount_amount     DOUBLE PRECISION,
    net_revenue         DOUBLE PRECISION,
    total_amount        DOUBLE PRECISION,
    order_year          INTEGER,
    order_month         INTEGER,
    order_day           INTEGER,
    order_hour          INTEGER,
    day_of_week         INTEGER,
    is_weekend          BOOLEAN,
    order_item_count    BIGINT,
    order_total_revenue DOUBLE PRECISION,
    is_high_value       BOOLEAN,
    etl_processed_at    TIMESTAMP,
    etl_process_date    DATE
);

-- Read-only group for analysts; dbt's post_hook GRANTs marts to it.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analytics_readers') THEN
        CREATE ROLE analytics_readers NOLOGIN;
    END IF;
END $$;
GRANT USAGE ON SCHEMA analytics TO analytics_readers;
