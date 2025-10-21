"""
Cassandra sink — stores scored transactions for real-time lookups.

Cassandra is used for low-latency queries during fraud scoring:
- Recent transactions by customer (velocity checks)
- Customer profiles with rolling stats
- Known countries/devices per customer
"""

import json
from datetime import datetime, timezone

import structlog
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, SimpleStatement
from confluent_kafka import Consumer

from config.settings import settings

logger = structlog.get_logger()

SETUP_CQL = """
CREATE KEYSPACE IF NOT EXISTS fraud_detection
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS fraud_detection.transactions (
    customer_id text,
    transaction_id text,
    amount double,
    merchant text,
    category text,
    country text,
    fraud_score double,
    is_fraud boolean,
    event_time timestamp,
    scored_at timestamp,
    PRIMARY KEY ((customer_id), event_time, transaction_id)
) WITH CLUSTERING ORDER BY (event_time DESC)
  AND default_time_to_live = 2592000;  -- 30 days TTL

CREATE TABLE IF NOT EXISTS fraud_detection.customer_stats (
    customer_id text PRIMARY KEY,
    total_transactions counter,
    total_fraud counter,
    last_transaction_time timestamp,
    known_countries set<text>,
    known_devices set<text>
);

CREATE TABLE IF NOT EXISTS fraud_detection.fraud_alerts (
    alert_date date,
    scored_at timestamp,
    transaction_id text,
    customer_id text,
    amount double,
    fraud_score double,
    country text,
    merchant text,
    PRIMARY KEY ((alert_date), scored_at, transaction_id)
) WITH CLUSTERING ORDER BY (scored_at DESC);
"""


def get_session():
    """Create Cassandra session."""
    auth = PlainTextAuthProvider(
        username=settings.cassandra.username,
        password=settings.cassandra.password,
    )
    cluster = Cluster(
        settings.cassandra.host_list,
        port=settings.cassandra.port,
        auth_provider=auth,
    )
    session = cluster.connect()
    return session, cluster


def setup_schema():
    """Create keyspace and tables."""
    session, cluster = get_session()
    for stmt in SETUP_CQL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            session.execute(stmt)
    logger.info("cassandra_schema_created")
    cluster.shutdown()


def run_cassandra_sink():
    """Consume scored transactions and write to Cassandra."""
    session, cluster = get_session()
    session.set_keyspace("fraud_detection")

    insert_txn = session.prepare("""
        INSERT INTO transactions
            (customer_id, transaction_id, amount, merchant, category,
             country, fraud_score, is_fraud, event_time, scored_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    insert_alert = session.prepare("""
        INSERT INTO fraud_alerts
            (alert_date, scored_at, transaction_id, customer_id,
             amount, fraud_score, country, merchant)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "group.id": "cassandra-sink",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([settings.kafka.topic_scored])

    batch_size = 50
    batch = []
    total = 0

    logger.info("cassandra_sink_started")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if batch:
                    _flush_batch(session, insert_txn, insert_alert, batch)
                    total += len(batch)
                    batch = []
                continue
            if msg.error():
                logger.error("consumer_error", error=msg.error())
                continue

            txn = json.loads(msg.value().decode("utf-8"))
            batch.append(txn)

            if len(batch) >= batch_size:
                _flush_batch(session, insert_txn, insert_alert, batch)
                total += len(batch)
                batch = []

                if total % 500 == 0:
                    logger.info("cassandra_sink_progress", total=total)

    except KeyboardInterrupt:
        if batch:
            _flush_batch(session, insert_txn, insert_alert, batch)
            total += len(batch)
        logger.info("cassandra_sink_stopped", total=total)
    finally:
        consumer.close()
        cluster.shutdown()


def _flush_batch(session, insert_txn, insert_alert, batch):
    """Write a batch of transactions to Cassandra."""
    for txn in batch:
        scored_at = datetime.fromisoformat(
            txn.get("scored_at", datetime.now(timezone.utc).isoformat()).replace("Z", "+00:00")
        )
        event_time = datetime.fromisoformat(
            txn.get("timestamp", scored_at.isoformat()).replace("Z", "+00:00")
        )

        session.execute(
            insert_txn,
            (
                txn["customer_id"],
                txn["transaction_id"],
                txn["amount"],
                txn.get("merchant", ""),
                txn.get("category", ""),
                txn.get("country", "US"),
                txn.get("fraud_score", 0.0),
                txn.get("is_fraud", False),
                event_time,
                scored_at,
            ),
        )

        if txn.get("is_fraud"):
            session.execute(
                insert_alert,
                (
                    scored_at.date(),
                    scored_at,
                    txn["transaction_id"],
                    txn["customer_id"],
                    txn["amount"],
                    txn["fraud_score"],
                    txn.get("country", "US"),
                    txn.get("merchant", ""),
                ),
            )


if __name__ == "__main__":
    setup_schema()
    run_cassandra_sink()
