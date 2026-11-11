import json
import pytest

from consumer import consumer as c


# ============================================================
# MOCK OBJECTS
# ============================================================

class FakeCursor:

    def __init__(self):
        self.executed_sql = None
        self.executed_values = None
        self.rowcount = 1

    def execute(self, sql, values):
        self.executed_sql = sql
        self.executed_values = values

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class FakeConnection:

    def __init__(self):
        self.fake_cursor = FakeCursor()
        self.closed = False
        self.commit_count = 0
        self.rollback_count = 0

    def cursor(self):
        return self.fake_cursor

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1

    def close(self):
        self.closed = True


class FakeKafkaMessage:

    def __init__(
        self,
        topic="ipfix-bng-1",
        value=None,
        partition=0,
        offset=100,
        timestamp=123456789,
    ):
        self.topic = topic
        self.value = value
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp


class FakeKafkaConsumer:

    def __init__(self, messages):
        self.messages = messages
        self.closed = False

    def __iter__(self):
        return iter(self.messages)

    def close(self):
        self.closed = True


class FakeKafkaProducer:

    def __init__(self):
        self.closed = False
        self.sent = []

    def send(self, topic, value=None, key=None):
        self.sent.append({
            "topic": topic,
            "value": value,
            "key": key,
        })

        class Future:
            def get(self, timeout=None):
                return None

        return Future()

    def close(self):
        self.closed = True


class FailOnceCursor:

    def __init__(self):
        self.attempts = 0
        self.rowcount = 1

    def execute(self, sql, values):
        self.attempts += 1

        if self.attempts == 1:
            raise RuntimeError("Temporary PostgreSQL failure")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class FailOnceConnection(FakeConnection):

    def __init__(self):
        super().__init__()
        self.fake_cursor = FailOnceCursor()

# ============================================================
# HELPERS
# ============================================================

def create_ipfix_message(**extra_fields):
    data = {
        "type": "IPFIX",
        "sampler_address": "10.3.4.1",
        "observation_domain_id": 100,
        "src_addr": "10.10.10.1",
        "dst_addr": "8.8.8.8",
    }
    data.update(extra_fields)
    return data


def create_kafka_message(data, topic="ipfix-bng-1"):
    if data is None:
        value = None
    elif isinstance(data, bytes):
        value = data
    else:
        value = json.dumps(data).encode("utf-8")

    return FakeKafkaMessage(topic=topic, value=value)


def get_raw_json(connection):
    """Return the original Python object passed to Json(...)."""
    return connection.fake_cursor.executed_values[7].adapted


# ============================================================
# FIXTURE – exercises main() end-to-end
# ============================================================

@pytest.fixture
def consumer_environment(monkeypatch):
    inserted = []
    dlq = []
    committed = []

    connection = FakeConnection()
    kafka_consumer = FakeKafkaConsumer([])
    kafka_producer = FakeKafkaProducer()

    def fake_create_consumer():
        return kafka_consumer

    def fake_create_producer():
        return kafka_producer

    def fake_insert_with_retry(conn, cluster_id, message, data):
        inserted.append({
            "cluster_id": cluster_id,
            "message": message,
            "data": data,
        })
        return connection, None

    def fake_publish_to_dlq(**kwargs):
        dlq.append(kwargs)

    def fake_commit_message(consumer, message):
        committed.append(message)

    monkeypatch.setattr(c, "create_consumer", fake_create_consumer)
    monkeypatch.setattr(c, "create_producer", fake_create_producer)
    monkeypatch.setattr(c, "insert_with_retry", fake_insert_with_retry)
    monkeypatch.setattr(c, "publish_to_dlq", fake_publish_to_dlq)
    monkeypatch.setattr(c, "commit_message", fake_commit_message)

    return {
        "consumer": kafka_consumer,
        "producer": kafka_producer,
        "connection": connection,
        "inserted": inserted,
        "dlq": dlq,
        "committed": committed,
    }


# ============================================================
# Happy path through main()
# ============================================================

def test_valid_ipfix_message_is_stored_and_committed(consumer_environment):
    env = consumer_environment
    message = create_kafka_message(create_ipfix_message())

    env["consumer"].messages = [message]
    c.main()

    assert len(env["inserted"]) == 1
    assert env["inserted"][0]["data"]["type"] == "IPFIX"
    assert env["inserted"][0]["cluster_id"] == "bng-1"
    assert len(env["dlq"]) == 0
    assert message in env["committed"]


# ============================================================
# Invalid messages → DLQ + commit (never reach Postgres)
# ============================================================

@pytest.mark.parametrize(
    "invalid_value",
    [
        pytest.param({"type": "NETFLOW"}, id="wrong-type"),
        pytest.param(b"{invalid-json", id="invalid-json"),
        pytest.param(None, id="null-message"),
        pytest.param(["IPFIX", "10.3.4.1"], id="root-array"),
        pytest.param("IPFIX", id="root-string"),
    ],
)
def test_invalid_messages_go_to_dlq_and_are_committed(
    consumer_environment, invalid_value
):
    env = consumer_environment
    message = create_kafka_message(invalid_value)

    env["consumer"].messages = [message]
    c.main()

    assert len(env["inserted"]) == 0
    assert len(env["dlq"]) == 1

    dlq_record = env["dlq"][0]
    assert dlq_record["error_stage"] == "message_validation"
    assert dlq_record["retry_count"] == 0
    assert message in env["committed"]


# ============================================================
# Cluster ID is derived from topic name
# ============================================================

@pytest.mark.parametrize(
    "topic, expected_cluster",
    [
        ("ipfix-bng-1", "bng-1"),
        ("ipfix-bng-2", "bng-2"),
        ("ipfix-dia-1", "dia-1"),
    ],
)
def test_cluster_id_extracted_from_topic(
    consumer_environment, topic, expected_cluster
):
    env = consumer_environment
    message = create_kafka_message(create_ipfix_message(), topic=topic)

    env["consumer"].messages = [message]
    c.main()

    assert len(env["inserted"]) == 1
    assert env["inserted"][0]["cluster_id"] == expected_cluster


# ============================================================
# Device identification priority
# ============================================================

@pytest.mark.parametrize(
    "fields, expected_device",
    [
        pytest.param(
            {
                "sampler_address": "10.3.4.1",
                "device": "fallback-device",
                "exporter_name": "exporter-1",
            },
            "10.3.4.1",
            id="prefers-sampler_address",
        ),
        pytest.param(
            {
                "device": "BNG-DEVICE-1",
                "exporter_name": "exporter-1",
            },
            "BNG-DEVICE-1",
            id="falls-back-to-device",
        ),
        pytest.param(
            {"exporter_name": "BNG-EXPORTER-1"},
            "BNG-EXPORTER-1",
            id="falls-back-to-exporter_name",
        ),
        pytest.param({}, None, id="no-device-identifier"),
    ],
)
def test_device_identification_priority(fields, expected_device):
    connection = FakeConnection()
    data = {"type": "IPFIX", **fields,}

    c.insert_flow(connection, "bng-1", FakeKafkaMessage(), data)

    values = connection.fake_cursor.executed_values
    assert values[1] == expected_device


# ============================================================
# Full fidelity of the stored JSON payload
# ============================================================

def test_unknown_fields_nested_objects_and_arrays_are_preserved():
    connection = FakeConnection()

    data = create_ipfix_message(
        future_field="future-value",
        custom_metadata={"region": "lab", "interface": "CORE"},
        metadata={
            "router": {
                "name": "BNG1",
                "interface": {"name": "CORE", "status": "UP"},
            }
        },
        interfaces=["CORE", "ACCESS", "UPLINK"],
        addresses=["10.10.10.1", "10.10.10.2"],
    )

    c.insert_flow(connection, "bng-1", FakeKafkaMessage(), data)

    stored = get_raw_json(connection)

    assert stored["future_field"] == "future-value"
    assert stored["custom_metadata"]["region"] == "lab"
    assert stored["metadata"]["router"]["name"] == "BNG1"
    assert stored["metadata"]["router"]["interface"]["status"] == "UP"
    assert stored["interfaces"] == ["CORE", "ACCESS", "UPLINK"]
    assert stored["addresses"] == ["10.10.10.1", "10.10.10.2"]


def test_missing_optional_fields_become_null_but_raw_json_is_kept():
    connection = FakeConnection()
    data = {"type": "IPFIX"}

    c.insert_flow(connection, "bng-1", FakeKafkaMessage(), data)

    values = connection.fake_cursor.executed_values

    assert values[1] is None          # device
    assert values[5] is None          # time_received_ns
    assert values[6] is None          # observation_domain_id
    assert get_raw_json(connection) == data


# ============================================================
# Duplicate handling (ON CONFLICT DO NOTHING)
# ============================================================

def test_duplicate_message_is_still_committed(consumer_environment, monkeypatch):
    """
    When Postgres reports rowcount == 0 (duplicate), the consumer must
    still commit the offset and must NOT send anything to the DLQ.
    """
    env = consumer_environment

    def fake_insert_with_retry_duplicate(conn, cluster_id, message, data):
        # Simulate the real insert_flow returning False (duplicate)
        env["inserted"].append({
            "cluster_id": cluster_id,
            "message": message,
            "data": data,
            "was_duplicate": True,
        })
        return env["connection"], None

    monkeypatch.setattr(c, "insert_with_retry", fake_insert_with_retry_duplicate)

    message = create_kafka_message(create_ipfix_message())
    env["consumer"].messages = [message]
    c.main()

    assert len(env["inserted"]) == 1
    assert env["inserted"][0]["was_duplicate"] is True
    assert len(env["dlq"]) == 0
    assert message in env["committed"]


# ============================================================
# Postgres failure path → DLQ + commit
# ============================================================

def test_postgres_failure_goes_to_dlq_and_is_committed(
    consumer_environment, monkeypatch
):
    """
    After insert_with_retry exhausts retries, the message must be
    published to the DLQ and the offset must still be committed.
    """
    env = consumer_environment
    db_error = RuntimeError("connection refused")

    def fake_insert_with_retry_fail(conn, cluster_id, message, data):
        return None, db_error

    monkeypatch.setattr(c, "insert_with_retry", fake_insert_with_retry_fail)

    message = create_kafka_message(create_ipfix_message())
    env["consumer"].messages = [message]
    c.main()

    assert len(env["inserted"]) == 0
    assert len(env["dlq"]) == 1

    dlq_record = env["dlq"][0]
    assert dlq_record["error_stage"] == "postgres_insert"
    assert dlq_record["retry_count"] == c.DB_MAX_RETRIES
    assert dlq_record["error"] is db_error
    assert message in env["committed"]


# ============================================================
# insert_flow itself – column mapping correctness
# ============================================================

def test_insert_flow_column_mapping():
    connection = FakeConnection()
    data = create_ipfix_message(
        time_received_ns=1715000000000000000,
        observation_domain_id=42,
    )
    message = FakeKafkaMessage(
        topic="ipfix-bng-1",
        partition=3,
        offset=999,
    )

    inserted = c.insert_flow(connection, "bng-1", message, data)

    assert inserted is True
    values = connection.fake_cursor.executed_values

    assert values[0] == "bng-1"                          # cluster_id
    assert values[1] == "10.3.4.1"                       # device
    assert values[2] == "ipfix-bng-1"                    # source_topic
    assert values[3] == 3                                # source_partition
    assert values[4] == 999                              # source_offset
    assert values[5] == 1715000000000000000              # time_received_ns
    assert values[6] == 42                               # observation_domain_id
    assert get_raw_json(connection) == data              # raw_data


def test_multiple_devices_in_one_topic(consumer_environment):

    env = consumer_environment

    messages = [
        create_kafka_message(
            {"type": "IPFIX", "sampler_address": "10.3.4.1"}
        ),
        create_kafka_message(
            {"type": "IPFIX", "sampler_address": "10.3.4.2"}
        ),
        create_kafka_message(
            {"type": "IPFIX", "sampler_address": "10.3.4.3"}
        ),
    ]

    env["consumer"].messages = messages

    c.main()

    assert len(env["inserted"]) == 3

    devices = [
        record["data"]["sampler_address"]
        for record in env["inserted"]
    ]

    assert devices == [
        "10.3.4.1",
        "10.3.4.2",
        "10.3.4.3",
    ]

    assert all(
        record["cluster_id"] == "bng-1"
        for record in env["inserted"]
    )


# ============================================================
# ACTUAL duplicate handling
# ============================================================

def test_insert_flow_returns_false_for_duplicate():
    """
    Simulate PostgreSQL ON CONFLICT DO NOTHING.

    PostgreSQL returns rowcount = 0 when the record already exists.
    insert_flow() should return False.
    """

    connection = FakeConnection()
    connection.fake_cursor.rowcount = 0

    data = create_ipfix_message()
    message = FakeKafkaMessage(
        topic="ipfix-bng-1",
        partition=0,
        offset=100,
    )

    inserted = c.insert_flow(
        connection,
        "bng-1",
        message,
        data,
    )

    assert inserted is False
    assert connection.fake_cursor.executed_sql is not None

    assert "ON CONFLICT" in connection.fake_cursor.executed_sql
    assert "DO NOTHING" in connection.fake_cursor.executed_sql


def test_insert_with_retry_commits_duplicate_without_error(
    monkeypatch,
):
    """
    A duplicate is not a database failure.

    insert_flow() returns False, but insert_with_retry()
    should treat it as successfully processed and commit.
    """

    connection = FakeConnection()
    connection.fake_cursor.rowcount = 0

    monkeypatch.setattr(
        c.time,
        "sleep",
        lambda delay: None,
    )

    data = create_ipfix_message()
    message = FakeKafkaMessage()

    returned_connection, error = c.insert_with_retry(
        connection,
        "bng-1",
        message,
        data,
    )

    assert returned_connection is connection
    assert error is None

    assert connection.commit_count == 1
    assert connection.rollback_count == 0
    assert connection.closed is False


# ============================================================
# ACTUAL retry behaviour
# ============================================================

def test_insert_with_retry_recovers_after_temporary_db_failure(
    monkeypatch,
):
    """
    First database attempt fails.
    Second database attempt succeeds.

    Verify that:
    - The failed connection is rolled back and closed.
    - A new connection is created.
    - The second attempt succeeds.
    - The successful connection is committed.
    """

    first_connection = FailOnceConnection()
    second_connection = FakeConnection()

    connections = iter([second_connection])

    # The retry function should obtain a new connection
    # after the first connection fails.
    monkeypatch.setattr(
        c,
        "connect_postgres",
        lambda: next(connections),
    )

    # Avoid waiting during the retry test.
    monkeypatch.setattr(
        c.time,
        "sleep",
        lambda delay: None,
    )

    data = create_ipfix_message()
    message = FakeKafkaMessage()

    returned_connection, error = c.insert_with_retry(
        first_connection,
        "bng-1",
        message,
        data,
    )

    assert error is None
    assert returned_connection is second_connection

    # First connection failed.
    assert first_connection.rollback_count == 1
    assert first_connection.closed is True

    # Second connection succeeded.
    assert second_connection.commit_count == 1
    assert second_connection.closed is False


def test_insert_with_retry_exhausts_all_attempts(
    monkeypatch,
):
    """
    Every database attempt fails.

    Verify that:
    - The configured number of attempts is respected.
    - Failed connections are rolled back and closed.
    - The final database error is returned.
    """

    failed_connections = []

    class AlwaysFailCursor:

        def __init__(self):
            self.rowcount = 1

        def execute(self, sql, values):
            raise RuntimeError("PostgreSQL unavailable")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            pass

    class AlwaysFailConnection(FakeConnection):

        def __init__(self):
            super().__init__()
            self.fake_cursor = AlwaysFailCursor()

    def fake_get_db_connection():
        connection = AlwaysFailConnection()
        failed_connections.append(connection)
        return connection

    monkeypatch.setattr(
        c,
        "connect_postgres",
        fake_get_db_connection,
    )

    monkeypatch.setattr(
        c.time,
        "sleep",
        lambda delay: None,
    )

    initial_connection = AlwaysFailConnection()

    data = create_ipfix_message()
    message = FakeKafkaMessage()

    returned_connection, error = c.insert_with_retry(
        initial_connection,
        "bng-1",
        message,
        data,
    )

    assert returned_connection is None
    assert error is not None
    assert "PostgreSQL unavailable" in str(error)

    # Initial connection + subsequent retry connections.
    assert len(failed_connections) == c.DB_MAX_RETRIES - 1

    assert initial_connection.closed is True
    assert initial_connection.rollback_count == 1

    for connection in failed_connections:
        assert connection.closed is True
        assert connection.rollback_count == 1

        

# ============================================================
# ACTUAL DLQ publishing
# ============================================================

def test_publish_to_dlq_sends_message_to_kafka():

    producer = FakeKafkaProducer()

    message = FakeKafkaMessage(
        topic="ipfix-bng-1",
        partition=2,
        offset=456,
        timestamp=123456789,
    )

    original_message = {
        "type": "IPFIX",
        "sampler_address": "10.3.4.1",
        "src_addr": "10.10.10.1",
        "dst_addr": "8.8.8.8",
    }

    error = RuntimeError("PostgreSQL connection refused")

    c.publish_to_dlq(
        producer=producer,
        message=message,
        cluster_id="bng-1",
        error_stage="postgres_insert",
        error=error,
        retry_count=c.DB_MAX_RETRIES,
        original_message=original_message,
    )

    # Verify that exactly one message was sent.
    assert len(producer.sent) == 1

    sent_message = producer.sent[0]

    # Verify destination topic.
    assert sent_message["topic"] == c.KAFKA_DLQ_TOPIC

    # The production implementation does not set a Kafka key.
    assert sent_message["key"] is None

    # Our fake producer receives the value before serialization.
    dlq_record = sent_message["value"]

    assert isinstance(dlq_record, dict)

    # Cluster information.
    assert dlq_record["cluster_id"] == "bng-1"

    # Original Kafka message coordinates.
    assert dlq_record["source"]["topic"] == "ipfix-bng-1"
    assert dlq_record["source"]["partition"] == 2
    assert dlq_record["source"]["offset"] == 456
    assert dlq_record["source"]["timestamp"] == 123456789

    # Failure information.
    assert dlq_record["failure"]["stage"] == "postgres_insert"
    assert dlq_record["failure"]["error_type"] == "RuntimeError"

    assert (
        dlq_record["failure"]["error_message"]
        == "PostgreSQL connection refused"
    )

    assert (
        dlq_record["failure"]["retry_count"]
        == c.DB_MAX_RETRIES
    )

    assert "failed_at" in dlq_record["failure"]

    # Original IPFIX payload must be preserved.
    assert dlq_record["original_message"] == original_message


def test_publish_to_dlq_raises_when_kafka_publish_fails():

    class FailedFuture:

        def get(self, timeout=None):
            raise RuntimeError("Kafka broker unavailable")

    class FailedKafkaProducer:

        def send(self, topic, value=None):
            return FailedFuture()

    producer = FailedKafkaProducer()

    message = FakeKafkaMessage()

    with pytest.raises(
        RuntimeError,
        match="Kafka broker unavailable",
    ):
        c.publish_to_dlq(
            producer=producer,
            message=message,
            cluster_id="bng-1",
            error_stage="message_validation",
            error=ValueError("Invalid IPFIX message"),
            retry_count=0,
            original_message={"type": "NETFLOW"},
        )

# ============================================================
# DLQ failure must not commit the original Kafka message
# ============================================================

def test_main_does_not_commit_when_dlq_publishing_fails(
    consumer_environment,
    monkeypatch,
):

    env = consumer_environment

    db_error = RuntimeError("PostgreSQL unavailable")

    def fake_insert_with_retry(conn, cluster_id, message, data):
        return None, db_error

    def fake_publish_to_dlq(**kwargs):
        raise RuntimeError("Kafka DLQ unavailable")

    monkeypatch.setattr(
        c,
        "insert_with_retry",
        fake_insert_with_retry,
    )

    monkeypatch.setattr(
        c,
        "publish_to_dlq",
        fake_publish_to_dlq,
    )

    message = create_kafka_message(create_ipfix_message())

    env["consumer"].messages = [message]

    with pytest.raises(
        RuntimeError,
        match="Kafka DLQ unavailable",
    ):
        c.main()

    # Original message must not be committed.
    assert message not in env["committed"]

    # Database failure must have been handled through the DLQ path.
    assert len(env["dlq"]) == 0