from pydantic_settings import BaseSettings


class KafkaSettings(BaseSettings):
    bootstrap_servers: str = "localhost:9092"
    topic_transactions: str = "txn.raw"
    topic_scored: str = "txn.scored"
    topic_fraud_alerts: str = "txn.fraud_alerts"
    topic_dlq: str = "txn.dlq"

    model_config = {"env_prefix": "KAFKA_"}


class CassandraSettings(BaseSettings):
    hosts: str = "localhost"
    port: int = 9042
    keyspace: str = "fraud_detection"
    username: str = "cassandra"
    password: str = "cassandra"

    model_config = {"env_prefix": "CASSANDRA_"}

    @property
    def host_list(self) -> list[str]:
        return [h.strip() for h in self.hosts.split(",")]


class RedshiftSettings(BaseSettings):
    host: str = ""
    port: int = 5439
    db: str = "fraud_analytics"
    user: str = "admin"
    password: str = ""
    schema_name: str = "public"

    model_config = {"env_prefix": "REDSHIFT_"}


class FraudRuleSettings(BaseSettings):
    score_threshold: float = 0.7
    high_amount_threshold: float = 5000.0
    velocity_window_minutes: int = 10
    velocity_max_count: int = 5
    card_test_amount_threshold: float = 5.0
    card_test_window_minutes: int = 5
    card_test_min_count: int = 3

    model_config = {"env_prefix": "FRAUD_"}


class Settings(BaseSettings):
    kafka: KafkaSettings = KafkaSettings()
    cassandra: CassandraSettings = CassandraSettings()
    redshift: RedshiftSettings = RedshiftSettings()
    fraud: FraudRuleSettings = FraudRuleSettings()
    slack_webhook_url: str = ""
    spark_master: str = "spark://spark-master:7077"
    spark_checkpoint_dir: str = "/tmp/spark-checkpoints"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
