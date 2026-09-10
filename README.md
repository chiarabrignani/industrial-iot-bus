# Industrial IoT Bus

Sistema di monitoraggio della telemetria di autobus basato su **MQTT, Apache Kafka, Apache Flink, PostgreSQL e Grafana**.

Il progetto simula una flotta di autobus che invia continuamente dati di telemetria. I dati vengono acquisiti tramite MQTT, trasferiti su Kafka, elaborati in tempo reale da Flink e infine salvati in PostgreSQL. Durante l'elaborazione vengono inoltre individuate alcune condizioni anomale e generati eventi di allarme.

Grafana viene utilizzato per la visualizzazione dei dati e per il monitoraggio della pipeline attraverso una dashboard aggiornata automaticamente.

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
* salvare gli alert rilevati in PostgreSQL;
* visualizzare statistiche e dati tramite Grafana;
* verificare automaticamente la logica di generazione degli alert tramite test Python.

Le principali anomalie gestite sono:

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
+--------+---------+
         |
         | SQL
         v
+------------------+
| Grafana          |
| Dashboard        |
+------------------+
```

Il flusso principale della telemetria è:

```text
Bus Simulator
      ↓
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
   Grafana
```

Gli alert seguono inoltre il percorso:

```text
             Flink
            /     \
           ↓       ↓
PostgreSQL        Kafka
  (alerts)     (bus-alerts)
     ↓
  Grafana
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

Il broker Kafka è configurato con tre partizioni per il topic di telemetria.

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
4. controlla la presenza di condizioni anomale;
5. genera gli eventuali alert;
6. salva gli alert in PostgreSQL;
7. pubblica gli alert sul topic Kafka `bus-alerts`.

Il job utilizza una `ProcessFunction` di PyFlink per elaborare i messaggi e mantenere una connessione PostgreSQL durante l'esecuzione.

La logica di controllo degli alert è stata separata in:

```text
flink/jobs/alert_logic.py
```

La funzione principale è:

```python
check_alerts(data)
```

Per permettere al worker Python di Flink di utilizzare il modulo, il job distribuisce esplicitamente il file tramite `add_python_file()`.

---

### PostgreSQL

PostgreSQL contiene due tabelle principali:

```text
telemetry
alerts
```

La tabella `telemetry` contiene i dati ricevuti dagli autobus.

La tabella `alerts` contiene gli eventi anomali rilevati.

La colonna `event_id` identifica univocamente una telemetria.

---

### Grafana

Grafana viene utilizzato come strumento di **monitoraggio e visualizzazione** dei dati presenti in PostgreSQL.

È stata configurata una dashboard:

```text
Industrial IoT - Bus Monitoring
```

La dashboard contiene attualmente i seguenti pannelli:

* **Telemetrie Totali**;
* **Alert Totali**;
* **Telemetrie per Autobus**;
* **Ultimi Alert**;
* **Velocità Media per Autobus**;
* **Temperatura Media per Autobus**;
* **Ultime Telemetrie**.

La dashboard utilizza PostgreSQL come Data Source e viene aggiornata automaticamente ogni **5 secondi**.

La dashboard permette quindi di osservare in tempo reale l'evoluzione delle telemetrie e degli alert prodotti dalla pipeline.

---

## 4. Struttura del progetto

```text
industrial-iot-bus/
│
├── README.md
├── docker-compose.yml
├── .gitignore
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── bridge/
│   ├── Dockerfile
│   ├── bridge.py
│   └── requirements.txt
│
├── flink/
│   ├── Dockerfile
│   └── jobs/
│       ├── telemetry_job.py
│       └── alert_logic.py
│
├── mqtt/
│   └── mosquitto.conf
│
├── postgres/
│   └── init.sql
│
├── simulator/
│   ├── Dockerfile
│   ├── bus_simulator.py
│   └── requirements.txt
│
└── tests/
    ├── test_alert_logic.py
    ├── test_telemetry_job.py
    └── test_integration.py
```

La configurazione e i dati persistenti di Grafana vengono conservati nel volume Docker:

```text
grafana_data
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
grafana
```

Grafana è disponibile sulla porta:

```text
3000
```

mentre il JobManager di Flink è disponibile sulla porta:

```text
8081
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

Dopo un riavvio dei container, il job Flink deve essere nuovamente sottomesso manualmente con il comando precedente.

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
SELECT
    bus_id,
    timestamp,
    speed,
    engine_temperature,
    fuel_level,
    passengers
FROM telemetry
WHERE bus_id IN ('BUS_001', 'BUS_002', 'BUS_003')
ORDER BY timestamp DESC
LIMIT 10;
```

È stato verificato che il simulatore genera continuamente nuovi dati e che questi vengono salvati correttamente in PostgreSQL.

È stata inoltre verificata la crescita progressiva del numero di telemetrie mentre il job Flink era in esecuzione.

---

## 9. Statistiche SQL

Sono state utilizzate query SQL per ottenere alcune statistiche sulla flotta.

### Numero di telemetrie per autobus

```sql
SELECT
    bus_id,
    COUNT(*) AS numero_telemetrie
FROM telemetry
WHERE bus_id IN ('BUS_001', 'BUS_002', 'BUS_003')
GROUP BY bus_id
ORDER BY bus_id;
```

### Velocità media e massima

```sql
SELECT
    bus_id,
    ROUND(AVG(speed)::numeric, 2) AS velocita_media,
    MAX(speed) AS velocita_massima
FROM telemetry
WHERE bus_id IN ('BUS_001', 'BUS_002', 'BUS_003')
GROUP BY bus_id
ORDER BY bus_id;
```

### Temperatura media e massima

```sql
SELECT
    bus_id,
    ROUND(AVG(engine_temperature)::numeric, 2) AS temperatura_media,
    MAX(engine_temperature) AS temperatura_massima
FROM telemetry
WHERE bus_id IN ('BUS_001', 'BUS_002', 'BUS_003')
GROUP BY bus_id
ORDER BY bus_id;
```

Queste statistiche sono utilizzate anche dalla dashboard Grafana.

---

## 10. Gestione degli alert

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

La logica è implementata nella funzione:

```text
check_alerts()
```

presente nel file:

```text
flink/jobs/alert_logic.py
```

---

## 11. Topic Kafka

Per visualizzare le telemetrie presenti in Kafka:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic bus-telemetry \
  --bootstrap-server kafka:9092 \
  --from-beginning
```

Per visualizzare gli alert:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic bus-alerts \
  --bootstrap-server kafka:9092 \
  --from-beginning
```

Per visualizzare i topic:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server kafka:9092
```

I topic principali sono:

```text
__consumer_offsets
bus-alerts
bus-telemetry
```

---

## 12. Test automatici

La logica degli alert è stata separata dal job Flink per poter essere verificata tramite test automatici.

I test unitari sono organizzati in due file:

```text
tests/test_alert_logic.py
tests/test_telemetry_job.py
```

Il file `test_alert_logic.py` contiene 8 test dedicati alla funzione `check_alerts()`, verificando i principali casi di funzionamento:

1. telemetria normale;
2. superamento della soglia di velocità;
3. superamento della soglia di temperatura;
4. presenza contemporanea delle due anomalie;
5. valori esattamente uguali alle soglie;
6. superamento della soglia di velocità di poco;
7. superamento della soglia di temperatura di poco;
8. correttezza completa del dizionario di alert prodotto.

Il file `test_telemetry_job.py` contiene invece 6 test dedicati al componente `TelemetryProcessor`. Vengono verificati:

1. apertura della connessione PostgreSQL;
2. inserimento di una telemetria;
3. inserimento di un alert;
4. elaborazione di una telemetria anomala e generazione degli alert;
5. gestione degli errori tramite `rollback()`;
6. chiusura della connessione e del cursor.

I test utilizzano dei mock per isolare il codice dalle dipendenze esterne, evitando di dover avviare realmente Flink o collegarsi a PostgreSQL durante gli unit test.

Per eseguire i test unitari:

```bash
python -m pytest -q tests/test_alert_logic.py tests/test_telemetry_job.py
```

L'esecuzione verificata ha prodotto:

```text
14 passed
```

È stata inoltre verificata la correttezza sintattica dei principali file Python tramite:

```bash
python -m py_compile flink/jobs/alert_logic.py
python -m py_compile flink/jobs/telemetry_job.py
python -m py_compile simulator/bus_simulator.py
python -m py_compile bridge/bridge.py
```

Tutti i controlli sintattici sono stati superati.

---

## 13. Test di integrazione

Il progetto include cinque test di integrazione che verificano il corretto funzionamento della pipeline tra **Kafka, Flink e PostgreSQL**, oltre alla pubblicazione degli alert sul topic Kafka `bus-alerts`.

I test si trovano in:

```text
tests/test_integration.py
```

Sono stati implementati cinque scenari:

1. **telemetria normale**: verifica che una telemetria con valori normali venga salvata in PostgreSQL senza generare alert;
2. **OVERSPEED**: verifica che una velocità superiore alla soglia generi solamente l'alert `OVERSPEED`;
3. **ENGINE_OVERHEATING**: verifica che una temperatura del motore superiore alla soglia generi solamente l'alert `ENGINE_OVERHEATING`;
4. **due alert**: verifica che il superamento contemporaneo delle due soglie generi entrambi gli alert;
5. **pubblicazione su Kafka**: verifica che Flink pubblichi correttamente l'alert generato sul topic Kafka `bus-alerts`.

Ogni scenario relativo alla verifica di PostgreSQL:

1. genera una telemetria di test con un identificativo univoco;
2. pubblica la telemetria sul topic Kafka `bus-telemetry`;
3. attende che Flink elabori il messaggio;
4. verifica che la telemetria sia stata salvata in PostgreSQL;
5. verifica che gli alert presenti siano esattamente quelli attesi;
6. elimina i dati creati dal test.

Il controllo degli alert in PostgreSQL utilizza un confronto esatto tra gli alert rilevati e quelli attesi. In questo modo il test fallisce anche nel caso in cui venga generato un alert aggiuntivo non previsto.

Il quinto test verifica invece il ramo di uscita degli alert da Flink verso Kafka. Una telemetria con velocità superiore alla soglia viene inviata sul topic `bus-telemetry`; il test verifica quindi che Flink generi l'alert `OVERSPEED` e lo pubblichi sul topic `bus-alerts`. La verifica utilizza l'`event_id` della telemetria per assicurarsi che l'alert ricevuto corrisponda esattamente all'evento generato dal test.

I test possono essere eseguiti tramite il servizio Docker dedicato:

```bash
docker compose run --rm integration-tests
```

L'esecuzione verificata ha prodotto:

```text
5 passed, 6 warnings in 8.67s
```

I cinque test di integrazione verificano quindi i seguenti scenari:

```text
Telemetria normale
    ↓
Kafka → Flink → PostgreSQL
    ↓
Nessun alert

OVERSPEED
    ↓
Kafka → Flink → PostgreSQL
    ↓
OVERSPEED

ENGINE_OVERHEATING
    ↓
Kafka → Flink → PostgreSQL
    ↓
ENGINE_OVERHEATING

Due anomalie contemporanee
    ↓
Kafka → Flink → PostgreSQL
    ↓
OVERSPEED + ENGINE_OVERHEATING

Pubblicazione alert
    ↓
Kafka bus-telemetry → Flink → Kafka bus-alerts
    ↓
OVERSPEED
```

Nel complesso, i test verificano entrambi i rami principali di output di Flink:

```text
                         ┌──→ PostgreSQL
                         │
Kafka bus-telemetry → Flink
                         │
                         └──→ Kafka bus-alerts
```

I dati creati dai test vengono inoltre eliminati al termine di ogni esecuzione, evitando di modificare permanentemente i dati utilizzati dal resto della piattaforma.

---

## 14. Coverage dei test

La copertura del codice viene misurata tramite `pytest-cov`.

Il comando utilizzato è:

```bash
python -m pytest \
    --cov=alert_logic \
    --cov=telemetry_job \
    --cov-report=term-missing \
    -q tests/test_alert_logic.py tests/test_telemetry_job.py
```

L'esecuzione verificata ha prodotto:

```text
Name                          Stmts   Miss  Cover   Missing
-----------------------------------------------------------
flink/jobs/alert_logic.py         7      0   100%
flink/jobs/telemetry_job.py      47      9    81%   153-206, 210
-----------------------------------------------------------
TOTAL                            54      9    83%

14 passed
```

La funzione `check_alerts()`, contenuta in `alert_logic.py`, risulta quindi coperta al 100%.

Il modulo `telemetry_job.py` raggiunge invece una coverage dell'81%. Le istruzioni non coperte riguardano principalmente la funzione `main()`, che contiene la configurazione della pipeline Flink e viene verificata a livello di integrazione anziché tramite unit test.

La coverage complessiva dei moduli analizzati è pari all'83%.

---

## 15. CI/CD con GitHub Actions

Il progetto utilizza **GitHub Actions** per automatizzare le verifiche del codice e la preparazione dell'ambiente containerizzato.

La pipeline è definita nel file:

```text
.github/workflows/ci.yml
```

La pipeline viene eseguita automaticamente:

* quando viene effettuato un `push` sul branch `master`;
* quando viene aperta o aggiornata una `pull request` verso `master`.

La pipeline è organizzata in due job principali.

Il job `test` esegue le attività di **Continuous Integration**:

1. checkout del repository;
2. configurazione dell'ambiente Python;
3. installazione delle dipendenze di test;
4. verifica della sintassi dei file Python;
5. esecuzione dei test unitari;
6. misurazione della coverage.

Se il job `test` termina con successo, viene eseguito il job `docker`, che verifica la configurazione di Docker Compose e costruisce le immagini Docker del progetto:

1. verifica della configurazione tramite `docker compose config -q`;
2. build delle immagini tramite `docker compose build`.

La dipendenza tra i due job garantisce quindi che la fase Docker venga eseguita solo dopo il superamento dei test.

L'esecuzione della pipeline è stata verificata con successo tramite GitHub Actions, con entrambi i job `test` e `docker` terminati con esito positivo.

Il test di integrazione è inoltre disponibile come servizio Docker e può essere eseguito localmente con:

```bash
docker compose run --rm integration-tests
```

Il deployment e l'avvio dell'intera piattaforma vengono gestiti tramite Docker Compose, che permette di eseguire i diversi componenti in container.

---

## 16. Test end-to-end degli alert

È stato eseguito un test completo inviando manualmente una telemetria anomala sul topic Kafka `bus-telemetry`.

La telemetria di test utilizzata conteneva:

```text
bus_id = BUS_TEST_ALERT
speed = 120.0 km/h
engine_temperature = 110.0 °C
```

Il risultato atteso era:

```text
OVERSPEED
ENGINE_OVERHEATING
```

### PostgreSQL

La telemetria è stata salvata nella tabella:

```text
telemetry
```

e sono state generate due righe nella tabella:

```text
alerts
```

una per ciascuna anomalia.

### Kafka

Gli stessi due alert sono stati pubblicati sul topic:

```text
bus-alerts
```

Il test ha quindi verificato il seguente percorso:

```text
bus-telemetry
      ↓
    Flink
      ↓
   ┌──┴───┐
   ↓      ↓
PostgreSQL Kafka
 alerts  bus-alerts
```

---

## 17. Test della dashboard Grafana

La connessione tra Grafana e PostgreSQL è stata verificata con successo tramite:

```text
Database Connection OK
```

La dashboard utilizza PostgreSQL come Data Source.

È stato inoltre verificato che:

1. il simulatore produce nuove telemetrie;
2. Flink elabora i messaggi;
3. PostgreSQL riceve nuove righe;
4. il numero di telemetrie aumenta nel tempo;
5. Grafana aggiorna automaticamente i pannelli.

Il refresh della dashboard è impostato a:

```text
5 secondi
```

È stato verificato che il valore di **Telemetrie Totali** aumenta sia in PostgreSQL sia nella dashboard Grafana.

---

## 18. Gestione dei duplicati

La tabella `telemetry` utilizza `event_id` come chiave primaria.

Per evitare errori nel caso in cui Kafka/Flink rielabori un evento già presente, l'inserimento della telemetria utilizza:

```sql
ON CONFLICT (event_id) DO NOTHING
```

In questo modo una telemetria già presente non viene inserita nuovamente.

La tabella `alerts` non utilizza attualmente lo stesso meccanismo, perché una singola telemetria può generare più alert differenti.

---

## 19. Arresto del progetto

Per fermare i container senza rimuoverli:

```bash
docker compose stop
```

Per riavviarli successivamente:

```bash
docker compose start
```

Dopo il riavvio dei container, verificare lo stato:

```bash
docker ps
```

e controllare il job Flink:

```bash
docker exec flink-jobmanager flink list
```

Se non risultano job in esecuzione, sottomettere nuovamente:

```bash
docker exec flink-jobmanager \
  flink run -py /opt/flink/telemetry_job.py
```

Per mantenere i dati PostgreSQL e la configurazione persistente di Grafana, evitare comandi che rimuovano i volumi Docker.

---

## 20. Stato attuale

Attualmente sono implementati e verificati:

* [x] Simulazione della telemetria degli autobus
* [x] Comunicazione MQTT
* [x] MQTT broker Mosquitto
* [x] Bridge MQTT → Kafka
* [x] Topic Kafka `bus-telemetry`
* [x] Elaborazione con Apache Flink
* [x] Rilevamento `OVERSPEED`
* [x] Rilevamento `ENGINE_OVERHEATING`
* [x] Generazione automatica di anomalie nel simulatore
* [x] Topic Kafka `bus-alerts`
* [x] Database PostgreSQL
* [x] Salvataggio delle telemetrie
* [x] Salvataggio degli alert
* [x] Gestione dei duplicati delle telemetrie
* [x] Separazione della logica degli alert
* [x] Test automatici della logica degli alert
* [x] 14 test unitari superati
* [x] 5 test di integrazione Kafka → Flink → PostgreSQL e Kafka `bus-alerts`
* [x] Test end-to-end della pipeline
* [x] Coverage di `alert_logic.py` al 100%
* [x] Coverage di `telemetry_job.py` all'81%
* [x] Coverage complessiva dei moduli analizzati all'83%
* [x] Pipeline CI/CD tramite GitHub Actions
* [x] Dashboard Grafana
* [x] Collegamento Grafana → PostgreSQL
* [x] Aggiornamento automatico della dashboard
* [x] Visualizzazione delle telemetrie
* [x] Visualizzazione degli alert
* [x] Query SQL per statistiche sulla flotta
* [x] Deployment dei componenti tramite Docker Compose

### Sviluppi successivi

Possibili estensioni del progetto:

* gestione più robusta degli errori e delle riconnessioni PostgreSQL;
* gestione più robusta dei duplicati degli alert;
* ulteriori tipologie di anomalie;
* monitoraggio più avanzato tramite Grafana;
* configurazione automatica dell'avvio del job Flink;
* aggiunta di ulteriori metriche operative;
* eventuale persistenza/versionamento della configurazione della dashboard Grafana.
