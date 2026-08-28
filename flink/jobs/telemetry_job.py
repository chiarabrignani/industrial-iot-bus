import json
import psycopg2

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Types


def save_telemetry(data):
    """
    Salva una telemetria nella tabella telemetry di PostgreSQL.
    """

    connection = psycopg2.connect(
        host="postgres",
        port=5432,
        database="busdb",
        user="bususer",
        password="buspassword"
    )

    cursor = connection.cursor()

    cursor.execute("""
        INSERT INTO telemetry (
            event_id,
            bus_id,
            timestamp,
            latitude,
            longitude,
            speed,
            engine_on,
            engine_temperature,
            fuel_level,
            passengers
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        data["event_id"],
        data["bus_id"],
        data["timestamp"],
        data["latitude"],
        data["longitude"],
        data["speed"],
        data["engine_on"],
        data["engine_temperature"],
        data["fuel_level"],
        data["passengers"]
    ))

    connection.commit()

    cursor.close()
    connection.close()


def save_alert(alert):
    """
    Salva un alert nella tabella alerts di PostgreSQL.
    """

    connection = psycopg2.connect(
        host="postgres",
        port=5432,
        database="busdb",
        user="bususer",
        password="buspassword"
    )

    cursor = connection.cursor()

    cursor.execute("""
        INSERT INTO alerts (
            event_id,
            bus_id,
            alert_type,
            value,
            threshold,
            timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        alert["event_id"],
        alert["bus_id"],
        alert["alert_type"],
        alert["value"],
        alert["threshold"],
        alert["timestamp"]
    ))

    connection.commit()

    cursor.close()
    connection.close()


def check_telemetry(message):
    """
    Riceve un messaggio JSON di telemetria
    e restituisce gli eventuali alert.
    """

    try:
        data = json.loads(message)

        save_telemetry(data)

        bus_id = data["bus_id"]
        event_id = data["event_id"]
        timestamp = data["timestamp"]

        speed = data["speed"]
        temperature = data["engine_temperature"]

        alerts = []

        # Controllo velocità
        if speed > 90:
            alert = {
                "event_id": event_id,
                "bus_id": bus_id,
                "alert_type": "OVERSPEED",
                "value": speed,
                "threshold": 90.0,
                "timestamp": timestamp
            }

            save_alert(alert)
            
            alerts.append(json.dumps(alert))

        # Controllo temperatura
        if temperature > 100:
            alert = {
                "event_id": event_id,
                "bus_id": bus_id,
                "alert_type": "ENGINE_OVERHEATING",
                "value": temperature,
                "threshold": 100.0,
                "timestamp": timestamp
            }

            save_alert(alert)

            alerts.append(json.dumps(alert))

        return alerts

    except Exception as e:
        print(f"Errore nell'elaborazione del messaggio: {e}")
        return []


def main():

    # Ambiente Flink
    env = StreamExecutionEnvironment.get_execution_environment()

    # --------------------------------------------------
    # KAFKA SOURCE
    # --------------------------------------------------

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("bus-telemetry") \
        .set_group_id("flink-telemetry-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Stream di telemetria
    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )

    # --------------------------------------------------
    # PROCESSING
    # --------------------------------------------------

    alerts = stream.flat_map(
        check_telemetry,
        output_type=Types.STRING()
    )

    # --------------------------------------------------
    # KAFKA SINK
    # --------------------------------------------------

    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("bus-alerts")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()

    # Invio degli alert a Kafka
    alerts.sink_to(kafka_sink)

    # --------------------------------------------------
    # AVVIO DEL JOB
    # --------------------------------------------------

    env.execute("Bus Telemetry Job")


if __name__ == "__main__":
    main()