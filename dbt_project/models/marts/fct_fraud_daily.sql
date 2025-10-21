{{ config(
    materialized='incremental',
    unique_key='txn_date',
    sort='txn_date',
    dist='txn_date'
) }}

with txns as (
    select * from {{ ref('stg_scored_transactions') }}
    {% if is_incremental() %}
    where txn_date > (select max(txn_date) from {{ this }})
    {% endif %}
)
select
    txn_date,
    count(*) as total_transactions,
    sum(case when is_fraud then 1 else 0 end) as fraud_count,
    round(sum(case when is_fraud then 1 else 0 end)::decimal / nullif(count(*), 0) * 100, 4) as fraud_rate_pct,
    round(sum(amount), 2) as total_amount,
    round(sum(case when is_fraud then amount else 0 end), 2) as fraud_amount,
    round(avg(fraud_score), 4) as avg_fraud_score,
    round(avg(case when is_fraud then fraud_score end), 4) as avg_fraud_score_flagged,
    count(distinct customer_id) as unique_customers,
    count(distinct case when is_fraud then customer_id end) as fraud_customers,
    count(distinct country) as unique_countries,
    round(avg(rules_fired), 2) as avg_rules_fired
from txns
group by txn_date
