import json
import os
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaProducer


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "bus-telemetry"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "busdb"
POSTGRES_USER = "bususer"
POSTGRES_PASSWORD = "buspassword"


def test_pipeline_kafka_flink_postgres():
    """
    Verifica l'integrazione tra Kafka, Flink e PostgreSQL.
    """

    event_id = str(uuid.uuid4())

    telemetry = {
        "event_id": event_id,
        "bus_id": "BUS_INTEGRATION_TEST",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latitude": 44.50,
        "longitude": 11.35,
        "speed": 120.0,
        "engine_on": True,
        "engine_temperature": 110.0,
        "fuel_level": 80.0,
        "passengers": 40
    }

    # Pubblica la telemetria su Kafka
    producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    request_timeout_ms=10000,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )

    producer.send(KAFKA_TOPIC, telemetry)
    producer.flush()
    producer.close()

    # Connessione a PostgreSQL
    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DATABASE,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

    cursor = connection.cursor()

    try:
        # Attende che Flink elabori il messaggio
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

            alerts_found = {
                "OVERSPEED",
                "ENGINE_OVERHEATING"
            }.issubset(alert_types)

            if telemetry_found and alerts_found:
                break

            time.sleep(1)

        assert telemetry_found, "Telemetria non trovata in PostgreSQL"
        assert alerts_found, "Alert attesi non trovati in PostgreSQL"

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