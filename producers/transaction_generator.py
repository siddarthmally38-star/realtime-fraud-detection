"""
Synthetic transaction generator — produces realistic payment transactions
with configurable fraud injection rate for pipeline testing.

Generates both legitimate and fraudulent transactions with patterns:
- Normal: typical amounts, regular merchants, home country
- Fraud: high amounts, velocity bursts, foreign countries, card testing
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

import structlog
from confluent_kafka import Producer
from faker import Faker

from config.settings import settings

logger = structlog.get_logger()
fake = Faker()

MERCHANTS = [
    "Amazon", "Walmart", "Target", "BestBuy", "Starbucks",
    "McDonalds", "Shell", "CVS", "Costco", "HomeDepot",
    "Apple Store", "Netflix", "Spotify", "Uber", "DoorDash",
]

CATEGORIES = [
    "retail", "groceries", "gas", "dining", "entertainment",
    "travel", "healthcare", "utilities", "subscription", "electronics",
]

COUNTRIES = ["US", "US", "US", "US", "US", "CA", "GB", "DE", "BR", "NG", "RU", "CN"]

CARD_TYPES = ["visa", "mastercard", "amex", "discover"]


def generate_customer_profile(customer_id: str) -> dict:
    """Generate a stable customer profile for consistent behavior."""
    random.seed(hash(customer_id) % (2**32))
    profile = {
        "customer_id": customer_id,
        "avg_amount": random.uniform(20, 500),
        "home_country": "US",
        "typical_merchants": random.sample(MERCHANTS, 5),
        "card_type": random.choice(CARD_TYPES),
    }
    random.seed()  # Reset seed
    return profile


def generate_normal_transaction(customer_id: str) -> dict:
    """Generate a legitimate-looking transaction."""
    profile = generate_customer_profile(customer_id)
    amount = max(1.0, random.gauss(profile["avg_amount"], profile["avg_amount"] * 0.3))

    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "amount": round(amount, 2),
        "currency": "USD",
        "merchant": random.choice(profile["typical_merchants"]),
        "category": random.choice(CATEGORIES),
        "country": profile["home_country"],
        "card_type": profile["card_type"],
        "card_last_four": fake.credit_card_number()[-4:],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ip_address": fake.ipv4(),
        "device_id": f"device_{hash(customer_id) % 10000:04d}",
        "is_online": random.random() > 0.4,
        "label": "legitimate",
    }


def generate_fraud_transaction(customer_id: str, fraud_type: str) -> dict:
    """Generate a fraudulent transaction based on fraud type."""
    txn = generate_normal_transaction(customer_id)
    txn["label"] = "fraud"

    if fraud_type == "high_amount":
        txn["amount"] = round(random.uniform(5000, 50000), 2)
        txn["merchant"] = random.choice(["Luxury Watches", "Gold Dealer", "Crypto Exchange"])

    elif fraud_type == "foreign_country":
        txn["country"] = random.choice(["NG", "RU", "CN", "BR"])
        txn["ip_address"] = fake.ipv4()

    elif fraud_type == "card_testing":
        txn["amount"] = round(random.uniform(0.50, 4.99), 2)
        txn["merchant"] = random.choice(["Test Merchant", "Digital Goods LLC"])

    elif fraud_type == "velocity":
        txn["amount"] = round(random.uniform(100, 2000), 2)

    return txn


def run_generator(
    tps: float = 10.0,
    fraud_rate: float = 0.05,
    n_customers: int = 1000,
):
    """Generate and publish transactions to Kafka.

    Args:
        tps: Transactions per second
        fraud_rate: Fraction of transactions that are fraudulent
        n_customers: Number of unique customers
    """
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "client.id": "txn-generator",
            "compression.type": "snappy",
        }
    )

    customer_ids = [str(uuid.uuid4()) for _ in range(n_customers)]
    fraud_types = ["high_amount", "foreign_country", "card_testing", "velocity"]
    total = 0
    fraud_count = 0

    logger.info(
        "generator_started",
        tps=tps,
        fraud_rate=fraud_rate,
        customers=n_customers,
    )

    try:
        while True:
            customer_id = random.choice(customer_ids)

            if random.random() < fraud_rate:
                fraud_type = random.choice(fraud_types)

                # Velocity: generate burst
                if fraud_type == "velocity":
                    for _ in range(random.randint(5, 10)):
                        txn = generate_fraud_transaction(customer_id, "velocity")
                        producer.produce(
                            topic=settings.kafka.topic_transactions,
                            key=customer_id.encode(),
                            value=json.dumps(txn).encode(),
                        )
                        fraud_count += 1
                        total += 1
                else:
                    txn = generate_fraud_transaction(customer_id, fraud_type)
                    producer.produce(
                        topic=settings.kafka.topic_transactions,
                        key=customer_id.encode(),
                        value=json.dumps(txn).encode(),
                    )
                    fraud_count += 1
                    total += 1
            else:
                txn = generate_normal_transaction(customer_id)
                producer.produce(
                    topic=settings.kafka.topic_transactions,
                    key=customer_id.encode(),
                    value=json.dumps(txn).encode(),
                )
                total += 1

            producer.poll(0)

            if total % 100 == 0:
                logger.info(
                    "generator_stats",
                    total=total,
                    fraud=fraud_count,
                    rate=f"{fraud_count/max(total,1)*100:.1f}%",
                )

            time.sleep(1.0 / tps)

    except KeyboardInterrupt:
        logger.info("generator_stopped", total=total, fraud=fraud_count)
    finally:
        producer.flush(timeout=10)


if __name__ == "__main__":
    run_generator()
