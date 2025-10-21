{{ config(materialized='view') }}

with raw as (
    select * from {{ source('fraud_raw', 'scored_transactions') }}
)
select
    transaction_id,
    customer_id,
    amount,
    currency,
    merchant,
    category,
    country,
    card_type,
    fraud_score,
    is_fraud,
    rules_fired,
    event_time,
    scored_at,
    DATE(event_time) as txn_date,
    EXTRACT(HOUR FROM event_time) as txn_hour,
    case
        when amount > 5000 then 'very_high'
        when amount > 1000 then 'high'
        when amount > 100 then 'medium'
        when amount > 10 then 'low'
        else 'micro'
    end as amount_bucket,
    case
        when fraud_score >= 0.9 then 'critical'
        when fraud_score >= 0.7 then 'high'
        when fraud_score >= 0.4 then 'medium'
        else 'low'
    end as risk_level
from raw
