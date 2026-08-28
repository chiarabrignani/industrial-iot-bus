import json
import random
import time
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt


# Configurazione MQTT
MQTT_BROKER = "mosquitto"
MQTT_PORT = 1883

# Autobus simulati
BUS_IDS = ["BUS_001", "BUS_002", "BUS_003"]


def genera_telemetria(bus_id):
    """
    Genera una misura di telemetria per un autobus.
    """

    dati = {
        "event_id": str(uuid.uuid4()),
        "bus_id": bus_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latitude": round(random.uniform(44.48, 44.51), 6),
        "longitude": round(random.uniform(11.32, 11.37), 6),
        "speed": round(random.uniform(0, 90), 1),
        "engine_on": True,
        "engine_temperature": round(random.uniform(70, 100), 1),
        "fuel_level": round(random.uniform(20, 100), 1),
        "passengers": random.randint(0, 80)
    }

    return dati


# Creazione client MQTT
client = mqtt.Client()

print("Connessione al broker MQTT...")

client.connect(MQTT_BROKER, MQTT_PORT, 60)

print("Connesso a Mosquitto!")


while True:

    for bus_id in BUS_IDS:

        # Generazione dati
        telemetria = genera_telemetria(bus_id)

        # Topic MQTT
        topic = f"bus/{bus_id}/telemetry"

        # Conversione del dizionario in JSON
        messaggio = json.dumps(telemetria)

        # Pubblicazione MQTT
        client.publish(topic, messaggio)

        print(f"[MQTT] {topic}")
        print(messaggio)
        print()

    print("-" * 60)

    # Attesa di 5 secondi
    time.sleep(5)