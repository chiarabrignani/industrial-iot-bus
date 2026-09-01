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

    Nella maggior parte dei casi vengono generati
    valori normali. Occasionalmente viene simulata
    una condizione anomala.
    """

    # Valori normali
    speed = round(random.uniform(0, 90), 1)
    engine_temperature = round(random.uniform(70, 100), 1)

    # Circa il 5% delle telemetrie contiene un'anomalia
    if random.random() < 0.05:

        tipo_anomalia = random.choice([
            "OVERSPEED",
            "ENGINE_OVERHEATING",
            "BOTH"
        ])

        if tipo_anomalia in ["OVERSPEED", "BOTH"]:
            speed = round(random.uniform(91, 120), 1)

        if tipo_anomalia in ["ENGINE_OVERHEATING", "BOTH"]:
            engine_temperature = round(random.uniform(101, 120), 1)

    dati = {
        "event_id": str(uuid.uuid4()),
        "bus_id": bus_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latitude": round(random.uniform(44.48, 44.51), 6),
        "longitude": round(random.uniform(11.32, 11.37), 6),
        "speed": speed,
        "engine_on": True,
        "engine_temperature": engine_temperature,
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