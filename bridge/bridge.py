import time

import paho.mqtt.client as mqtt
from kafka import KafkaProducer


# Configurazione MQTT
MQTT_BROKER = "mqtt-broker"
MQTT_PORT = 1883
MQTT_TOPIC = "bus/+/telemetry"


# Configurazione Kafka
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "bus-telemetry"


def create_kafka_producer():
    """
    Prova a collegarsi a Kafka.
    Se Kafka non è ancora disponibile,
    aspetta 5 secondi e riprova.
    """

    while True:
        try:
            print("Connessione a Kafka...")

            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER
            )

            print("Connesso a Kafka!")

            return producer

        except Exception as e:
            print(f"Kafka non ancora disponibile: {e}")
            print("Riprovo tra 5 secondi...")

            time.sleep(5)


# Creazione Kafka Producer
producer = create_kafka_producer()


def on_connect(client, userdata, flags, reason_code, properties):
    """
    Funzione chiamata quando il bridge
    si collega a Mosquitto.
    """

    print("Connesso a Mosquitto!")

    client.subscribe(MQTT_TOPIC)

    print(f"Sottoscritto al topic: {MQTT_TOPIC}")


def on_message(client, userdata, msg):
    """
    Funzione chiamata quando arriva
    un messaggio MQTT.
    """

    print()
    print("Messaggio MQTT ricevuto:")
    print(msg.topic)
    print(msg.payload.decode())

    producer.send(
        KAFKA_TOPIC,
        value=msg.payload
    )

    producer.flush()

    print(f"Messaggio inviato a Kafka: {KAFKA_TOPIC}")


# Creazione client MQTT
client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2
)

# Associazione delle funzioni agli eventi
client.on_connect = on_connect
client.on_message = on_message


print("Connessione a Mosquitto...")

client.connect(
    MQTT_BROKER,
    MQTT_PORT,
    60
)

# Avvia il loop MQTT
client.loop_forever()