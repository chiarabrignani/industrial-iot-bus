import json
import os
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaConsumer, KafkaProducer


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "bus-telemetry"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "busdb"
POSTGRES_USER = "bususer"
POSTGRES_PASSWORD = "buspassword"


def create_telemetry(speed, engine_temperature):
    """Crea una telemetria di test con valori personalizzati."""

    return {
        "event_id": str(uuid.uuid4()),
        "bus_id": "BUS_INTEGRATION_TEST",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latitude": 44.50,
        "longitude": 11.35,
        "speed": speed,
        "engine_on": True,
        "engine_temperature": engine_temperature,
        "fuel_level": 80.0,
        "passengers": 40
    }


def send_telemetry(telemetry):
    """Invia una telemetria sul topic Kafka."""

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        request_timeout_ms=10000,
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )

    producer.send(KAFKA_TOPIC, telemetry)
    producer.flush()
    producer.close()


def create_alert_consumer():
    """
    Crea un consumer Kafka dedicato al topic bus-alerts.
    """

    consumer = KafkaConsumer(
        "bus-alerts",
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        group_id=f"integration-test-{uuid.uuid4()}",
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        )
    )

    # Attende l'assegnazione della partizione.
    consumer.poll(timeout_ms=1000)

    return consumer


def check_alert_in_kafka(consumer, telemetry, expected_alert_type):
    """
    Verifica che Flink pubblichi l'alert atteso
    sul topic Kafka bus-alerts.
    """

    event_id = telemetry["event_id"]

    for _ in range(20):
        messages = consumer.poll(timeout_ms=1000)

        for records in messages.values():
            for message in records:
                alert = message.value

                if (
                    alert.get("event_id") == event_id
                    and alert.get("alert_type") == expected_alert_type
                ):
                    return

    raise AssertionError(
        f"Alert {expected_alert_type} non trovato "
        f"nel topic bus-alerts per l'evento {event_id}"
    )


def check_result(telemetry, expected_alerts):
    """
    Attende che Flink elabori la telemetria e verifica
    telemetria e alert presenti in PostgreSQL.
    """

    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DATABASE,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

    cursor = connection.cursor()

    event_id = telemetry["event_id"]

    try:
        telemetry_found = False
        alerts_found = False

        for _ in range(20):

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM telemetry
                WHERE event_id = %s
                """,
                (event_id,)
            )

            telemetry_count = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT alert_type
                FROM alerts
                WHERE event_id = %s
                """,
                (event_id,)
            )

            alert_types = {row[0] for row in cursor.fetchall()}

            telemetry_found = telemetry_count == 1

            alerts_found = alert_types == expected_alerts

            if telemetry_found and alerts_found:
                break

            time.sleep(1)

        assert telemetry_found, "Telemetria non trovata in PostgreSQL"
        assert alerts_found, (
            f"Alert attesi non trovati. "
            f"Attesi: {expected_alerts}, trovati: {alert_types}"
        )

    finally:
        # Elimina solamente i dati creati dal test
        cursor.execute(
            "DELETE FROM alerts WHERE event_id = %s",
            (event_id,)
        )

        cursor.execute(
            "DELETE FROM telemetry WHERE event_id = %s",
            (event_id,)
        )

        connection.commit()

        cursor.close()
        connection.close()


def test_pipeline_telemetria_normale():
    """
    Verifica che una telemetria normale venga salvata
    senza generare alert.
    """

    telemetry = create_telemetry(
        speed=50.0,
        engine_temperature=80.0
    )

    send_telemetry(telemetry)

    check_result(
        telemetry,
        expected_alerts=set()
    )


def test_pipeline_overspeed():
    """
    Verifica che una velocità superiore alla soglia
    generi solamente l'alert OVERSPEED.
    """

    telemetry = create_telemetry(
        speed=120.0,
        engine_temperature=80.0
    )

    send_telemetry(telemetry)

    check_result(
        telemetry,
        expected_alerts={"OVERSPEED"}
    )


def test_pipeline_engine_overheating():
    """
    Verifica che una temperatura del motore superiore alla soglia
    generi solamente l'alert ENGINE_OVERHEATING.
    """

    telemetry = create_telemetry(
        speed=50.0,
        engine_temperature=110.0
    )

    send_telemetry(telemetry)

    check_result(
        telemetry,
        expected_alerts={"ENGINE_OVERHEATING"}
    )


def test_pipeline_due_alert():
    """
    Verifica che il superamento contemporaneo delle due soglie
    generi entrambi gli alert.
    """

    telemetry = create_telemetry(
        speed=120.0,
        engine_temperature=110.0
    )

    send_telemetry(telemetry)

    check_result(
        telemetry,
        expected_alerts={
            "OVERSPEED",
            "ENGINE_OVERHEATING"
        }
    )


def test_pipeline_pubblica_alert_su_kafka():
    """
    Verifica che Flink pubblichi un alert sul topic Kafka bus-alerts.
    """

    telemetry = create_telemetry(
        speed=120.0,
        engine_temperature=80.0
    )

    consumer = create_alert_consumer()

    try:
        send_telemetry(telemetry)

        check_alert_in_kafka(
            consumer,
            telemetry,
            expected_alert_type="OVERSPEED"
        )

    finally:
        consumer.close()
