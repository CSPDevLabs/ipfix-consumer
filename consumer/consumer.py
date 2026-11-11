import json
import logging
import os
import random
import re
import time

from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition, OffsetAndMetadata


# ---------------------------------------------------------------------
# Kafka configuration
# ---------------------------------------------------------------------

KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "kafka.nok-ipfix.svc.cluster.local:9092",
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "ipfix-postgres-consumer",
)

KAFKA_DLQ_TOPIC = os.getenv(
    "KAFKA_DLQ_TOPIC",
    "ipfix-dlq",
)

# Refresh Kafka metadata periodically to discover newly created topics.
KAFKA_METADATA_MAX_AGE_MS = int(
    os.getenv("KAFKA_METADATA_MAX_AGE_MS", "30000")
)

# Automatically discover topics starting with "ipfix-".
# Exclude the DLQ topic to prevent consuming our own failed messages.
KAFKA_TOPIC_PATTERN = (
    rf"^(?!{re.escape(KAFKA_DLQ_TOPIC)}$)ipfix-.+$"
)


# ---------------------------------------------------------------------
# PostgreSQL configuration
# ---------------------------------------------------------------------

POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "ipfix-postgres.nok-ipfix.svc.cluster.local",
)

POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

POSTGRES_DB = os.getenv("POSTGRES_DB", "ipfix")
POSTGRES_USER = os.getenv("POSTGRES_USER", "ipfix")

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "ipfix-playground",
)

DB_MAX_RETRIES = int(os.getenv("DB_MAX_RETRIES", "5"))

DB_RETRY_BASE_SECONDS = float(
    os.getenv("DB_RETRY_BASE_SECONDS", "1")
)

DB_RETRY_MAX_SECONDS = float(
    os.getenv("DB_RETRY_MAX_SECONDS", "30")
)


# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# PostgreSQL connection
# ---------------------------------------------------------------------

def connect_postgres():

    logger.info(
        "Connecting to PostgreSQL %s:%s/%s",
        POSTGRES_HOST,
        POSTGRES_PORT,
        POSTGRES_DB,
    )

    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=5,
    )

    connection.autocommit = False

    logger.info("Connected to PostgreSQL")

    return connection


# ---------------------------------------------------------------------
# Generic IPFIX storage
# ---------------------------------------------------------------------

def insert_flow(connection, cluster_id, message, data):

    sql = """
        INSERT INTO ipfix_flows (
            cluster_id,
            device,
            source_topic,
            source_partition,
            source_offset,
            time_received_ns,
            observation_domain_id,
            raw_data
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)

        ON CONFLICT (
            source_topic,
            source_partition,
            source_offset
        ) DO NOTHING
    """

    # Device identification is optional.
    # We will confirm the correct GoFlow2 exporter fields later.
    device = (data.get("sampler_address") or data.get("device") or data.get("exporter_name"))

    values = (
        cluster_id,
        device,
        message.topic,
        message.partition,
        message.offset,
        data.get("time_received_ns"),
        data.get("observation_domain_id"),
        Json(data),
    )

    with connection.cursor() as cursor:
        cursor.execute(sql, values)

        # True = newly inserted
        # False = duplicate message skipped
        return cursor.rowcount == 1


# ---------------------------------------------------------------------
# Database insertion with retry and exponential backoff
# ---------------------------------------------------------------------

def insert_with_retry(connection, cluster_id, message, data):

    for attempt in range(1, DB_MAX_RETRIES + 1):

        try:
            if connection is None or connection.closed:
                connection = connect_postgres()

            inserted = insert_flow(
                connection,
                cluster_id,
                message,
                data,
            )

            connection.commit()

            if inserted:
                logger.info(
                    "Stored IPFIX record: cluster=%s topic=%s "
                    "partition=%s offset=%s",
                    cluster_id,
                    message.topic,
                    message.partition,
                    message.offset,
                )
            else:
                logger.info(
                    "Duplicate IPFIX record skipped: cluster=%s "
                    "topic=%s partition=%s offset=%s",
                    cluster_id,
                    message.topic,
                    message.partition,
                    message.offset,
                )

            return connection, None

        except Exception as exc:

            logger.error(
                "PostgreSQL attempt %s/%s failed: %s",
                attempt,
                DB_MAX_RETRIES,
                exc,
            )

            if connection is not None:
                try:
                    connection.rollback()
                    connection.close()
                except Exception:
                    pass

            connection = None

            if attempt == DB_MAX_RETRIES:
                return connection, exc

            # Exponential backoff with full jitter.
            maximum_delay = min(
                DB_RETRY_MAX_SECONDS,
                DB_RETRY_BASE_SECONDS * (2 ** (attempt - 1)),
            )

            retry_delay = random.uniform(0, maximum_delay)

            logger.info(
                "Retrying PostgreSQL in %.2f seconds",
                retry_delay,
            )

            time.sleep(retry_delay)

    return connection, RuntimeError("Database retries exhausted")


# ---------------------------------------------------------------------
# Kafka setup with automatic IPFIX topic discovery
# ---------------------------------------------------------------------

def create_consumer():

    logger.info(
        "Connecting to Kafka: %s",
        KAFKA_BROKER,
    )

    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BROKER],
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        metadata_max_age_ms=KAFKA_METADATA_MAX_AGE_MS,
    )

    # Subscribe using a regular expression.
    # Kafka automatically discovers newly created matching topics
    # when its metadata is refreshed.

    consumer.subscribe(
        pattern=KAFKA_TOPIC_PATTERN,
    )

    logger.info(
        "Subscribed to IPFIX topic pattern: %s",
        KAFKA_TOPIC_PATTERN,
    )

    logger.info(
        "Kafka metadata refresh interval: %s ms",
        KAFKA_METADATA_MAX_AGE_MS,
    )

    return consumer


def create_producer():

    logger.info("Creating Kafka DLQ producer")

    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        acks="all",
        retries=5,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


# ---------------------------------------------------------------------
# DLQ publishing
# ---------------------------------------------------------------------

def publish_to_dlq(
    producer,
    message,
    cluster_id,
    error_stage,
    error,
    retry_count,
    original_message,
):

    dlq_record = {
        "cluster_id": cluster_id,

        "source": {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
            "timestamp": message.timestamp,
        },

        "failure": {
            "stage": error_stage,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "retry_count": retry_count,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        },

        "original_message": original_message,
    }

    # Wait for Kafka to confirm that the DLQ message was delivered.
    producer.send(
        KAFKA_DLQ_TOPIC,
        value=dlq_record,
    ).get(timeout=30)

    logger.error(
        "Published failed record to DLQ: cluster=%s "
        "source_topic=%s partition=%s offset=%s",
        cluster_id,
        message.topic,
        message.partition,
        message.offset,
    )


# ---------------------------------------------------------------------
# Kafka offset commit
# ---------------------------------------------------------------------


def commit_message(consumer, message):

    topic_partition = TopicPartition(
        message.topic,
        message.partition,
    )

    # Kafka commits the NEXT offset to consume.
    # Therefore, commit the current message offset + 1.
    consumer.commit({
        topic_partition: OffsetAndMetadata(
            message.offset + 1,
            "",
            -1,
        )
    })

    logger.debug(
        "Committed Kafka offset: topic=%s partition=%s offset=%s",
        message.topic,
        message.partition,
        message.offset + 1,
    )



# ---------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------

def main():

    connection = None
    consumer = create_consumer()
    producer = create_producer()

    try:
        for message in consumer:

            # Each IPFIX topic represents one cluster.
            # Example: ipfix-bng-1 -> bng-1
            cluster_id = message.topic.removeprefix("ipfix-")

            try:
                # Handle Kafka tombstone/null messages.
                if message.value is None:
                    raise ValueError(
                        "Kafka message contains a null value"
                    )

                raw_message = message.value.decode("utf-8")
                data = json.loads(raw_message)

                if not isinstance(data, dict):
                    raise ValueError(
                        "IPFIX message must be a JSON object"
                    )

                if data.get("type") != "IPFIX":
                    raise ValueError(
                        f"Unexpected message type: {data.get('type')}"
                    )

            except Exception as exc:

                # Malformed or non-IPFIX records go to the shared DLQ.
                try:
                    original_message = (
                        message.value.decode(
                            "utf-8",
                            errors="replace",
                        )
                        if message.value is not None
                        else None
                    )

                    publish_to_dlq(
                        producer=producer,
                        message=message,
                        cluster_id=cluster_id,
                        error_stage="message_validation",
                        error=exc,
                        retry_count=0,
                        original_message=original_message,
                    )

                    commit_message(consumer, message)

                except Exception:
                    logger.exception(
                        "Could not publish invalid message to DLQ. "
                        "Stopping so the original message can be replayed."
                    )
                    raise

                continue

            # ---------------------------------------------------------
            # PostgreSQL insertion
            # ---------------------------------------------------------

            connection, db_error = insert_with_retry(
                connection,
                cluster_id,
                message,
                data,
            )

            if db_error is not None:

                try:
                    publish_to_dlq(
                        producer=producer,
                        message=message,
                        cluster_id=cluster_id,
                        error_stage="postgres_insert",
                        error=db_error,
                        retry_count=DB_MAX_RETRIES,
                        original_message=data,
                    )

                    # Commit only after DLQ delivery is confirmed.
                    commit_message(consumer, message)

                except Exception:
                    logger.exception(
                        "Could not publish failed database record "
                        "to DLQ. Stopping for safe replay."
                    )
                    raise

                continue

            # ---------------------------------------------------------
            # Successful PostgreSQL insertion
            # ---------------------------------------------------------

            commit_message(consumer, message)

    finally:

        if connection is not None:
            connection.close()

        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()