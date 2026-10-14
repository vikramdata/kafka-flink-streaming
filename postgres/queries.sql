-- Handy queries to explore the live results:
--   docker compose exec postgres psql -U postgres -d streaming -f /queries.sql

\echo '--- Revenue per minute (latest 10 windows) ---'
select window_start, channel, orders, revenue, avg_order_value
from realtime.revenue_per_minute
order by window_start desc, channel
limit 10;

\echo '--- Top categories, most recent 5-minute sliding window ---'
select window_start, window_end, category, orders, revenue
from realtime.category_sales_sliding
where window_end = (select max(window_end) from realtime.category_sales_sliding)
order by revenue desc;

\echo '--- Top 5 customers by live spend ---'
select customer_id, orders, total_spend, last_order_time
from realtime.customer_live_stats
order by total_spend desc
limit 5;

\echo '--- Latest high-value alerts ---'
select order_id, customer_id, category, amount, event_time
from realtime.order_alerts
order by event_time desc
limit 5;
