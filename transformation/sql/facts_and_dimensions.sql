-- Populate the stores dimension table
insert into public.stores_dim(store_group, store_token, store_name, start_date, end_date)
select store_group,store_token,store_name,now(),TO_TIMESTAMP('9999-12-31-00.00.00.000000', 'YYYY-MM-DD-HH24.MI.SS.US')
from public.stores where batch_date = '2020-01-27' ON CONFLICT (store_token)
    DO UPDATE SET store_name = excluded.store_name, store_group=  excluded.store_group;

-- Populate the role dimension table
insert into public.role_dim(user_role)
select user_role from sales
where batch_date = '2020-01-27' ON CONFLICT(user_role) do nothing;

-- Populate the sales fact table
insert into sales_fact(store_id, transaction_id, receipt_token, transaction_time, amount, source_id, role_id, batch_date)
select sd.store_id,
       s.transaction_id,
       s.receipt_token,
       to_timestamp(s.transaction_time,'YYYYMMDD"T"HH24MISS.MS'),
       cast(split_part(s.amount,'$',2) AS DECIMAL),
       s.source_id,
       rd.role_id, -- is it relevant ?
       s.batch_date
from sales s
inner join stores_dim sd on s.store_token = sd.store_token
inner join role_dim rd on s.user_role = rd.user_role
order by 4
ON CONFLICT(store_id,transaction_id)
    DO UPDATE SET receipt_token=excluded.receipt_token,
                  transaction_time=excluded.transaction_time,
                  amount=excluded.amount,
                  source_id=excluded.source_id,
                  role_id=excluded.role_id,
                  batch_date=excluded.batch_date;