-- Source: order events from Kafka.
-- The watermark tells Flink "events can arrive up to 10 seconds late". Windows close
-- once the watermark passes their end; events later than that are dropped.
CREATE TABLE orders (
    order_id     STRING,
    customer_id  INT,
    product_id   INT,
    category     STRING,
    channel      STRING,
    quantity     INT,
    unit_price   DECIMAL(10, 2),
    amount       DECIMAL(10, 2),
    event_time   TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-order-analytics',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.ignore-parse-errors' = 'true'
);
