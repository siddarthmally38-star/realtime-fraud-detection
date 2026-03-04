"""
Redshift sink — batches scored transactions into Redshift for analytics.

Uses micro-batch approach: accumulates records in memory, then performs
bulk INSERT via redshift_connector. This feeds the dbt analytics models.
"""

import json
from datetime import datetime, timezone

import structlog
import redshift_connector
from confluent_kafka import Consumer

from config.settings import settings

logger = structlog.get_logger()

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS scored_transactions (
    transaction_id VARCHAR(36) PRIMARY KEY,
    customer_id VARCHAR(36) NOT NULL,
    amount DECIMAL(12,2),
    currency VARCHAR(3),
    merchant VARCHAR(255),
    category VARCHAR(50),
    country VARCHAR(5),
    card_type VARCHAR(20),
    fraud_score DECIMAL(6,4),
    is_fraud BOOLEAN,
    rules_fired INT,
    event_time TIMESTAMP,
    scored_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (event_time);
"""


def get_connection():
    rs = settings.redshift
    return redshift_connector.connect(
        host=rs.host,
        port=rs.port,
        database=rs.db,
        user=rs.user,
        password=rs.password,
    )


def setup_table():
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    conn.close()
    logger.info("redshift_table_created")


def run_redshift_sink(batch_size: int = 100, flush_interval_seconds: int = 30):
    """Consume scored transactions and batch-insert to Redshift."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "group.id": "redshift-sink",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([settings.kafka.topic_scored])

    batch = []
    total = 0
    last_flush = datetime.now(timezone.utc)

    logger.info("redshift_sink_started", batch_size=batch_size)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg and not msg.error():
                txn = json.loads(msg.value().decode("utf-8"))
                batch.append(txn)

            elapsed = (datetime.now(timezone.utc) - last_flush).total_seconds()

            if len(batch) >= batch_size or (batch and elapsed >= flush_interval_seconds):
                _flush_to_redshift(batch)
                total += len(batch)
                batch = []
                last_flush = datetime.now(timezone.utc)

                if total % 500 == 0:
                    logger.info("redshift_sink_progress", total=total)

    except KeyboardInterrupt:
        if batch:
            _flush_to_redshift(batch)
            total += len(batch)
        logger.info("redshift_sink_stopped", total=total)
    finally:
        consumer.close()


def _flush_to_redshift(batch: list[dict]):
    """Bulk insert a batch of scored transactions."""
    if not batch:
        return

    conn = get_connection()
    insert_sql = """
        INSERT INTO scored_transactions
            (transaction_id, customer_id, amount, currency, merchant,
             category, country, card_type, fraud_score, is_fraud,
             rules_fired, event_time, scored_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    rows = []
    for txn in batch:
        rows.append((
            txn["transaction_id"],
            txn["customer_id"],
            txn.get("amount", 0),
            txn.get("currency", "USD"),
            txn.get("merchant", ""),
            txn.get("category", ""),
            txn.get("country", "US"),
            txn.get("card_type", ""),
            txn.get("fraud_score", 0),
            txn.get("is_fraud", False),
            txn.get("rules_fired", 0),
            txn.get("timestamp"),
            txn.get("scored_at"),
        ))

    with conn.cursor() as cur:
        cur.executemany(insert_sql, rows)
    conn.commit()
    conn.close()

    logger.debug("redshift_batch_flushed", rows=len(rows))


if __name__ == "__main__":
    setup_table()
    run_redshift_sink()
