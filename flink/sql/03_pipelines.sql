-- All pipelines run as ONE Flink job (a statement set), so the Kafka topic is read once
-- and fanned out to every query.
EXECUTE STATEMENT SET
BEGIN

-- 1. Tumbling window: revenue KPIs per channel for every 1-minute bucket.
INSERT INTO revenue_per_minute
SELECT
    window_start,
    window_end,
    channel,
    COUNT(*)                                      AS orders,
    CAST(SUM(quantity) AS BIGINT)                 AS units,
    CAST(SUM(amount) AS DECIMAL(12, 2))           AS revenue,
    CAST(AVG(amount) AS DECIMAL(12, 2))           AS avg_order_value
FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, channel;

-- 2. Hopping (sliding) window: last 5 minutes of sales per category, refreshed every minute.
INSERT INTO category_sales_sliding
SELECT
    window_start,
    window_end,
    category,
    COUNT(*)                                      AS orders,
    CAST(SUM(amount) AS DECIMAL(12, 2))           AS revenue
FROM TABLE(HOP(TABLE orders, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '5' MINUTES))
GROUP BY window_start, window_end, category;

-- 3. Stateful running aggregate: lifetime stats per customer, upserted as orders arrive.
INSERT INTO customer_live_stats
SELECT
    customer_id,
    COUNT(*)                                      AS orders,
    CAST(SUM(amount) AS DECIMAL(12, 2))           AS total_spend,
    MAX(event_time)                               AS last_order_time
FROM orders
GROUP BY customer_id;

-- 4. Real-time alerting: flag high-value orders (to Postgres and back to Kafka).
INSERT INTO order_alerts_db
SELECT order_id, customer_id, category, amount, event_time, 'HIGH_VALUE_ORDER' AS reason
FROM orders
WHERE amount >= 500;

INSERT INTO order_alerts_topic
SELECT order_id, customer_id, category, amount, event_time, 'HIGH_VALUE_ORDER' AS reason
FROM orders
WHERE amount >= 500;

END;
