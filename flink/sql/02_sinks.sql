-- Sinks. JDBC tables with a PRIMARY KEY run in upsert mode
-- (Postgres: INSERT ... ON CONFLICT DO UPDATE), so reprocessing never duplicates rows.

CREATE TABLE revenue_per_minute (
    window_start     TIMESTAMP(3),
    window_end       TIMESTAMP(3),
    channel          STRING,
    orders           BIGINT,
    units            BIGINT,
    revenue          DECIMAL(12, 2),
    avg_order_value  DECIMAL(12, 2),
    PRIMARY KEY (window_start, channel) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/streaming',
    'table-name' = 'realtime.revenue_per_minute',
    'username' = 'postgres',
    'password' = 'postgres'
);

CREATE TABLE category_sales_sliding (
    window_start  TIMESTAMP(3),
    window_end    TIMESTAMP(3),
    category      STRING,
    orders        BIGINT,
    revenue       DECIMAL(12, 2),
    PRIMARY KEY (window_end, category) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/streaming',
    'table-name' = 'realtime.category_sales_sliding',
    'username' = 'postgres',
    'password' = 'postgres'
);

CREATE TABLE customer_live_stats (
    customer_id      INT,
    orders           BIGINT,
    total_spend      DECIMAL(12, 2),
    last_order_time  TIMESTAMP(3),
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/streaming',
    'table-name' = 'realtime.customer_live_stats',
    'username' = 'postgres',
    'password' = 'postgres',
    'sink.buffer-flush.interval' = '2s'
);

CREATE TABLE order_alerts_db (
    order_id     STRING,
    customer_id  INT,
    category     STRING,
    amount       DECIMAL(10, 2),
    event_time   TIMESTAMP(3),
    reason       STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/streaming',
    'table-name' = 'realtime.order_alerts',
    'username' = 'postgres',
    'password' = 'postgres'
);

-- Alerts also go back into Kafka so downstream services could react to them.
CREATE TABLE order_alerts_topic (
    order_id     STRING,
    customer_id  INT,
    category     STRING,
    amount       DECIMAL(10, 2),
    event_time   TIMESTAMP(3),
    reason       STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'order_alerts',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'sink.delivery-guarantee' = 'at-least-once'
);
