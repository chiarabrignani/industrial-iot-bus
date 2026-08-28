# Industrial IoT Bus

Sistema di monitoraggio della telemetria di autobus basato su **MQTT, Apache Kafka, Apache Flink e PostgreSQL**.

Il progetto simula una flotta di autobus che invia continuamente dati di telemetria. I dati vengono acquisiti tramite MQTT, trasferiti su Kafka, elaborati in tempo reale da Flink e infine salvati in PostgreSQL. Durante l'elaborazione vengono inoltre individuate alcune condizioni anomale e generati eventi di allarme.

---

## 1. Obiettivo del progetto

L'obiettivo è realizzare una pipeline di **Industrial IoT** in grado di:

* simulare la telemetria di più autobus;
* trasmettere i dati tramite MQTT;
* trasferire i messaggi MQTT su Apache Kafka;
* elaborare la telemetria in tempo reale tramite Apache Flink;
* rilevare condizioni anomale;
* pubblicare gli alert su Kafka;
* salvare le telemetrie in PostgreSQL;
* salvare gli alert rilevati in PostgreSQL.

Le principali anomalie attualmente gestite sono:

* **OVERSPEED**: velocità superiore a 90 km/h;
* **ENGINE_OVERHEATING**: temperatura del motore superiore a 100 °C.

---

## 2. Architettura

La pipeline è composta dai seguenti componenti:

```text
+------------------+
| Bus Simulator    |
| Python           |
+--------+---------+
         |
         | MQTT
         v
+------------------+
| Mosquitto        |
| MQTT Broker      |
+--------+---------+
         |
         v
+------------------+
| MQTT-Kafka       |
| Bridge           |
+--------+---------+
         |
         | Kafka
         v
+------------------+
| bus-telemetry    |
| Kafka Topic      |
+--------+---------+
         |
         v
+------------------+
| Apache Flink     |
| Telemetry Job    |
+--------+---------+
         |
         +--------------------+
         |                    |
         |                    |
         v                    v
+------------------+   +------------------+
| PostgreSQL       |   | bus-alerts       |
| telemetry        |   | Kafka Topic      |
| alerts           |   +------------------+
+------------------+
```

---

## 3. Componenti

### Bus Simulator

Il simulatore è implementato in Python e genera dati di telemetria per tre autobus:

* `BUS_001`
* `BUS_002`
* `BUS_003`

Ogni messaggio contiene informazioni come:

* identificativo dell'evento;
* identificativo dell'autobus;
* timestamp;
* posizione GPS;
* velocità;
* stato del motore;
* temperatura del motore;
* livello del carburante;
* numero di passeggeri.

---

### Mosquitto

Mosquitto viene utilizzato come **MQTT broker**.

I messaggi di telemetria vengono pubblicati su topic del tipo:

```text
bus/BUS_001/telemetry
bus/BUS_002/telemetry
bus/BUS_003/telemetry
```

---

### MQTT-Kafka Bridge

Il bridge è un'applicazione Python che riceve i messaggi MQTT e li pubblica sul topic Kafka:

```text
bus-telemetry
```

In questo modo MQTT viene utilizzato per l'acquisizione dei dati, mentre Kafka costituisce il livello di messaggistica utilizzato dalla pipeline di elaborazione.

---

### Apache Kafka

Kafka gestisce due topic principali:

```text
bus-telemetry
bus-alerts
```

`bus-telemetry` contiene i dati prodotti dagli autobus.

`bus-alerts` contiene gli alert generati da Flink.

---

### Apache Flink

Flink esegue il job:

```text
Bus Telemetry Job
```

Il job:

1. legge le telemetrie da Kafka;
2. interpreta il messaggio JSON;
3. salva la telemetria in PostgreSQL;
4. controlla la velocità;
5. controlla la temperatura del motore;
6. genera gli eventuali alert;
7. salva gli alert in PostgreSQL;
8. pubblica gli alert sul topic Kafka `bus-alerts`.

Il job utilizza una `ProcessFunction` di PyFlink per mantenere una connessione PostgreSQL riutilizzabile durante l'elaborazione.

---

### PostgreSQL

PostgreSQL contiene due tabelle:

```text
telemetry
alerts
```

La tabella `telemetry` contiene i dati ricevuti dagli autobus.

La tabella `alerts` contiene gli eventi anomali rilevati.

---

## 4. Struttura del progetto

```text
industrial-iot-bus/
│
├── README.md
├── docker-compose.yml
├── .gitignore
│
├── bridge/
│   ├── Dockerfile
│   ├── bridge.py
│   └── requirements.txt
│
├── flink/
│   ├── Dockerfile
│   └── jobs/
│       └── telemetry_job.py
│
├── mqtt/
│   └── mosquitto.conf
│
├── postgres/
│   └── init.sql
│
└── simulator/
    ├── Dockerfile
    ├── bus_simulator.py
    └── requirements.txt
```

---

## 5. Avvio del progetto

Dalla directory principale del progetto:

```bash
cd ~/industrial-iot-bus
```

Per avviare i container:

```bash
docker compose up -d
```

Per verificare lo stato:

```bash
docker ps
```

I principali container sono:

```text
bus-simulator
mqtt-broker
mqtt-kafka-bridge
kafka
flink-jobmanager
flink-taskmanager
postgres
```

---

## 6. Avvio del Job Flink

Il job può essere sottomesso al JobManager con:

```bash
docker exec flink-jobmanager \
  flink run -py /opt/flink/telemetry_job.py
```

Per verificare lo stato del job:

```bash
docker exec flink-jobmanager flink list
```

Il job deve risultare:

```text
Bus Telemetry Job (RUNNING)
```

---

## 7. PostgreSQL

Per accedere al database:

```bash
docker exec -it postgres psql -U bususer -d busdb
```

Le tabelle principali sono:

```text
telemetry
alerts
```

Per visualizzare le tabelle:

```sql
\dt
```

Per verificare la struttura:

```sql
\d telemetry
\d alerts
```

---

## 8. Verifica delle telemetrie

Per controllare le ultime telemetrie degli autobus:

```sql
SELECT *
FROM telemetry
WHERE bus_id IN ('BUS_001', 'BUS_002', 'BUS_003')
ORDER BY timestamp DESC
LIMIT 10;
```

È stato verificato che il simulatore genera continuamente nuovi dati e che questi vengono salvati correttamente in PostgreSQL.

---

## 9. Gestione degli alert

### OVERSPEED

Viene generato quando:

```text
speed > 90
```

L'alert contiene:

```json
{
  "event_id": "...",
  "bus_id": "...",
  "alert_type": "OVERSPEED",
  "value": 120.0,
  "threshold": 90.0,
  "timestamp": "..."
}
```

### ENGINE_OVERHEATING

Viene generato quando:

```text
engine_temperature > 100
```

L'alert contiene:

```json
{
  "event_id": "...",
  "bus_id": "...",
  "alert_type": "ENGINE_OVERHEATING",
  "value": 110.0,
  "threshold": 100.0,
  "timestamp": "..."
}
```

Una singola telemetria può generare entrambi gli alert.

---

## 10. Topic Kafka

Per visualizzare le telemetrie presenti in Kafka:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic bus-telemetry \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

Per visualizzare gli alert:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic bus-alerts \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

Per visualizzare i topic:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

---

## 11. Test effettuati

La pipeline è stata verificata con test end-to-end.

È stata inviata una telemetria di test con:

```text
bus_id = BUS_TEST_3
speed = 120.0 km/h
engine_temperature = 110.0 °C
```

Il risultato atteso e osservato è stato:

```text
OVERSPEED
ENGINE_OVERHEATING
```

### PostgreSQL

La telemetria è stata salvata nella tabella:

```text
telemetry
```

con una singola riga relativa all'evento.

Sono inoltre state salvate due righe nella tabella:

```text
alerts
```

una per ciascuna anomalia.

### Kafka

Gli stessi due alert sono stati pubblicati sul topic:

```text
bus-alerts
```

Questo ha permesso di verificare l'intero percorso:

```text
MQTT
  ↓
Mosquitto
  ↓
MQTT-Kafka Bridge
  ↓
Kafka
  ↓
Flink
  ↓
PostgreSQL
  ↓
Kafka bus-alerts
```

---

## 12. Gestione dei duplicati

La tabella `telemetry` utilizza `event_id` come chiave primaria.

Per evitare errori nel caso in cui Kafka/Flink rielabori un evento già presente, l'inserimento della telemetria utilizza:

```sql
ON CONFLICT (event_id) DO NOTHING
```

In questo modo una telemetria già presente non viene inserita nuovamente.

La tabella `alerts` non utilizza attualmente lo stesso vincolo, perché una singola telemetria può generare più alert differenti.

---

## 13. Arresto del progetto

Per fermare i container senza rimuoverli:

```bash
docker compose stop
```

Per riavviarli successivamente:

```bash
docker compose start
```

Per mantenere i dati PostgreSQL, evitare di utilizzare comandi che rimuovano i volumi del database.

---

## 14. Stato attuale

Attualmente sono implementati e verificati:

* [x] Simulazione della telemetria degli autobus
* [x] Comunicazione MQTT
* [x] Bridge MQTT → Kafka
* [x] Topic Kafka `bus-telemetry`
* [x] Elaborazione con Apache Flink
* [x] Rilevamento `OVERSPEED`
* [x] Rilevamento `ENGINE_OVERHEATING`
* [x] Topic Kafka `bus-alerts`
* [x] Database PostgreSQL
* [x] Salvataggio delle telemetrie
* [x] Salvataggio degli alert
* [x] Gestione dei duplicati delle telemetrie
* [x] Test end-to-end della pipeline

### Sviluppi successivi

Possibili estensioni del progetto:

* test automatici;
* query SQL per statistiche e aggregazioni;
* dashboard per la visualizzazione dei dati;
* gestione più robusta degli errori e delle riconnessioni PostgreSQL;
* ulteriori tipologie di anomalie.
