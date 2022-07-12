-- Report Sales by Transaction
with
     month_amount_t as (
    select to_char(transaction_time, 'YYYYMM') as month_year,
           sum(amount) as total_amount_of_month
    from sales_fact group by 1
),
     top_store_token_t as (
         select trx_sales_stores.total_amount,
                trx_sales_stores.store_token,
                trx_sales_stores.store_id,
                trx_sales_stores.transaction_date,
                row_number() over (partition by trx_sales_stores.transaction_date order by trx_sales_stores.total_amount desc) as row_num
         from (select
                sum(amount) as total_amount,
                sd.store_token,
                sd.store_id,
                date(transaction_time) as transaction_date
         from sales_fact inner join stores_dim sd on sales_fact.store_id = sd.store_id
        group by 2,3,4)trx_sales_stores

     )
select date(sf.transaction_time) as transaction_date,
       count(sf.store_id) as total_stores,
       sum(sf.amount) as total_sales_amount,
       round(avg(sf.amount),2) as avg_sales_amount,
       mat.total_amount_of_month as trx_month_sales_amount,
       tst.store_token as top_store_sales_token
from sales_fact sf inner join month_amount_t mat
on mat.month_year = to_char(sf.transaction_time, 'YYYYMM')
inner join top_store_token_t tst on tst.store_id = sf.store_id and tst.row_num = 1
group by 1,5,6
order by 1 desc
limit 40;


--- Top Sales Store
with
      last_10_systems_days as (
         select distinct date(transaction_time) as date from sales_fact order by 1 desc limit 10
     ),
     top_storage_token as(
    select trx_sales_stores.total_amount,
    trx_sales_stores.store_token,
    trx_sales_stores.store_id,
    trx_sales_stores.transaction_date,
    trx_sales_stores.store_name,
    trx_sales_stores.total_sales,
    row_number() over (partition by trx_sales_stores.transaction_date order by trx_sales_stores.total_amount desc) as rank_id
    from (select
            sum(amount) as total_amount,
            count(amount) as total_sales,
            sd.store_token,
            sd.store_id,
            sd.store_name,
            date(transaction_time) as transaction_date
        from sales_fact inner join stores_dim sd on sales_fact.store_id = sd.store_id
        inner join last_10_systems_days lsd on lsd.date = date(sales_fact.transaction_time)
        group by 3,4,5,6)trx_sales_stores
)
select tst.transaction_date,
       tst.rank_id,
       tst.total_sales,
       tst.store_token,
       tst.store_name
       from top_storage_token tst
where tst.rank_id <= 5