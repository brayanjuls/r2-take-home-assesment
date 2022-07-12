/* RAW Tables */
create table public.sales
(
    store_token      varchar(40),
    transaction_id   varchar(40),
    receipt_token    varchar(30),
    transaction_time varchar(20),
    amount           varchar(30),
    source_id        varchar(40),
    user_role        varchar(30),
    batch_date       date,
    processing_date  date,
    primary key (store_token,transaction_id,batch_date)
);
create index sales_batch_date_index
    on sales (batch_date);


create table public.stores
(
    store_group     varchar(30),
    store_token     varchar(40),
    store_name      varchar(200),
    batch_date       date,
    processing_date  date,
    primary key (store_token,batch_date)
);
create index stores_batch_date_index
    on stores (batch_date);

/* Dimensional Modeling Tables */
create table public.stores_dim
(
    store_id      serial,
    store_group   varchar(30),
    store_token   varchar(40) UNIQUE,
    store_name    varchar(200),
    start_date    timestamp,
    end_date      timestamp,
    primary key (store_id)
);

create table public.role_dim
(
    role_id          serial,
    user_role        varchar(200),
    primary key (role_id)
);

create table public.sales_fact
(
    store_id      	 numeric,
    transaction_id   varchar(40),
    receipt_token    varchar(30),
    transaction_time timestamp,
    amount           numeric(11,2),
    source_id        varchar(40),
    role_id          numeric,
    batch_date      date,
    processing_date  date,
    primary key (store_id,transaction_id)
);

create index sales_fact_batch_date_index
    on sales_fact (batch_date);

create index sales_fact_transaction_time_index
    on sales_fact (transaction_time);

/* Reporting Tables */

create table report2
(
    transaction_date       date,
    total_stores           bigint,
    total_sales_amount     numeric,
    avg_sales_amount       numeric,
    trx_month_sales_amount numeric,
    top_store_sales_token  varchar(40),
    snapshot_date       date,
);

create index report2_transaction_date_index
    on report2 (transaction_date);
create index report2_snapshot_date_index
    on report2 (snapshot_date);

create table report3
(
    transaction_date date,
    rank_id          bigint,
    total_sales      bigint,
    store_token      varchar(40),
    store_name       varchar(200)
);
create index report3_transaction_date_index
    on report3 (transaction_date);