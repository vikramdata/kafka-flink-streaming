-- Target tables for the Flink JDBC sinks (Flink upserts on the primary keys).
create schema if not exists realtime;

create table if not exists realtime.revenue_per_minute (
    window_start     timestamp      not null,
    window_end       timestamp      not null,
    channel          text           not null,
    orders           bigint,
    units            bigint,
    revenue          numeric(12, 2),
    avg_order_value  numeric(12, 2),
    primary key (window_start, channel)
);

create table if not exists realtime.category_sales_sliding (
    window_start  timestamp  not null,
    window_end    timestamp  not null,
    category      text       not null,
    orders        bigint,
    revenue       numeric(12, 2),
    primary key (window_end, category)
);

create table if not exists realtime.customer_live_stats (
    customer_id      integer  primary key,
    orders           bigint,
    total_spend      numeric(12, 2),
    last_order_time  timestamp
);

create table if not exists realtime.order_alerts (
    order_id     text  primary key,
    customer_id  integer,
    category     text,
    amount       numeric(10, 2),
    event_time   timestamp,
    reason       text
);
