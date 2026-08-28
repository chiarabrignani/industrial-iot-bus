import json
import psycopg2

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessFunction

from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer
)

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Types


class TelemetryProcessor(ProcessFunction):
    """
    Elabora le telemetrie, salva i dati su PostgreSQL
    e genera eventuali alert.
    """

    def open(self, runtime_context):
        """
        Apre una connessione PostgreSQL quando
        l'operatore Flink viene inizializzato.
        """

        self.connection = psycopg2.connect(
            host="postgres",
            port=5432,
            database="busdb",
            user="bususer",
            password="buspassword"
        )

        self.cursor = self.connection.cursor()

        print("Connessione a PostgreSQL aperta.")

    def save_telemetry(self, data):
        """
        Salva una telemetria nella tabella telemetry.
        """

        self.cursor.execute("""
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
            ON CONFLICT (event_id) DO NOTHING
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

    def save_alert(self, alert):
        """
        Salva un alert nella tabella alerts.
        """

        self.cursor.execute("""
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

    def process_element(self, message, ctx):
        """
        Riceve una telemetria, la salva su PostgreSQL
        e restituisce gli eventuali alert.
        """

        try:
            data = json.loads(message)

            # ------------------------------------------
            # SALVATAGGIO TELEMETRIA
            # ------------------------------------------

            self.save_telemetry(data)

            bus_id = data["bus_id"]
            event_id = data["event_id"]
            timestamp = data["timestamp"]

            speed = data["speed"]
            temperature = data["engine_temperature"]

            alerts = []

            # ------------------------------------------
            # CONTROLLO VELOCITÀ
            # ------------------------------------------

            if speed > 90:

                alert = {
                    "event_id": event_id,
                    "bus_id": bus_id,
                    "alert_type": "OVERSPEED",
                    "value": speed,
                    "threshold": 90.0,
                    "timestamp": timestamp
                }

                self.save_alert(alert)

                alerts.append(json.dumps(alert))

            # ------------------------------------------
            # CONTROLLO TEMPERATURA
            # ------------------------------------------

            if temperature > 100:

                alert = {
                    "event_id": event_id,
                    "bus_id": bus_id,
                    "alert_type": "ENGINE_OVERHEATING",
                    "value": temperature,
                    "threshold": 100.0,
                    "timestamp": timestamp
                }

                self.save_alert(alert)

                alerts.append(json.dumps(alert))

            # ------------------------------------------
            # COMMIT POSTGRESQL
            # ------------------------------------------

            self.connection.commit()

            # ------------------------------------------
            # INVIO ALERT A KAFKA
            # ------------------------------------------

            for alert in alerts:
                yield alert

        except Exception as e:

            print(
                f"Errore nell'elaborazione del messaggio: {e}"
            )

            self.connection.rollback()

    def close(self):
        """
        Chiude la connessione PostgreSQL quando
        l'operatore Flink viene terminato.
        """

        if self.cursor:
            self.cursor.close()

        if self.connection:
            self.connection.close()

        print("Connessione a PostgreSQL chiusa.")


def main():

    # --------------------------------------------------
    # AMBIENTE FLINK
    # --------------------------------------------------

    env = StreamExecutionEnvironment.get_execution_environment()

    # --------------------------------------------------
    # KAFKA SOURCE
    # --------------------------------------------------

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("bus-telemetry") \
        .set_group_id("flink-telemetry-postgres") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )

    # --------------------------------------------------
    # PROCESSING + POSTGRESQL
    # --------------------------------------------------

    alerts = stream.process(
        TelemetryProcessor(),
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

    alerts.sink_to(kafka_sink)

    # --------------------------------------------------
    # AVVIO DEL JOB
    # --------------------------------------------------

    env.execute("Bus Telemetry Job")


if __name__ == "__main__":
    main()