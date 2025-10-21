{{ config(materialized='table', dist='customer_id') }}

with txns as (
    select * from {{ ref('stg_scored_transactions') }}
),
customer_stats as (
    select
        customer_id,
        count(*) as total_transactions,
        sum(case when is_fraud then 1 else 0 end) as fraud_count,
        round(sum(amount), 2) as total_amount,
        round(sum(case when is_fraud then amount else 0 end), 2) as fraud_amount,
        round(avg(fraud_score), 4) as avg_fraud_score,
        max(fraud_score) as max_fraud_score,
        min(event_time) as first_transaction,
        max(event_time) as last_transaction,
        count(distinct country) as unique_countries,
        count(distinct category) as unique_categories,
        count(distinct merchant) as unique_merchants
    from txns
    group by customer_id
)
select
    *,
    round(fraud_count::decimal / nullif(total_transactions, 0) * 100, 2) as fraud_rate_pct,
    case
        when fraud_count >= 5 or max_fraud_score >= 0.95 then 'blocked'
        when fraud_count >= 3 or avg_fraud_score >= 0.6 then 'high_risk'
        when fraud_count >= 1 or avg_fraud_score >= 0.3 then 'watch'
        else 'clean'
    end as risk_status
from customer_stats
