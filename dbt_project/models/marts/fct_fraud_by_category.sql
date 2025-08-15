{{ config(materialized='table', sort='fraud_rate_pct', dist='category') }}

with txns as (
    select * from {{ ref('stg_scored_transactions') }}
)
select
    category,
    count(*) as total_transactions,
    sum(case when is_fraud then 1 else 0 end) as fraud_count,
    round(sum(case when is_fraud then 1 else 0 end)::decimal / nullif(count(*), 0) * 100, 2) as fraud_rate_pct,
    round(sum(amount), 2) as total_amount,
    round(sum(case when is_fraud then amount else 0 end), 2) as fraud_amount,
    round(avg(amount), 2) as avg_transaction_amount,
    round(avg(case when is_fraud then amount end), 2) as avg_fraud_amount,
    count(distinct customer_id) as unique_customers
from txns
group by category
