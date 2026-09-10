import sys
from unittest.mock import MagicMock, patch


# --------------------------------------------------
# MOCK DI PYFLINK
# --------------------------------------------------

class FakeProcessFunction:
    pass


fake_datastream = MagicMock()
fake_datastream.StreamExecutionEnvironment = MagicMock()

fake_functions = MagicMock()
fake_functions.ProcessFunction = FakeProcessFunction

fake_kafka = MagicMock()
fake_common = MagicMock()
fake_serialization = MagicMock()


sys.modules["pyflink"] = MagicMock()
sys.modules["pyflink.datastream"] = fake_datastream
sys.modules["pyflink.datastream.functions"] = fake_functions
sys.modules["pyflink.datastream.connectors"] = MagicMock()
sys.modules["pyflink.datastream.connectors.kafka"] = fake_kafka
sys.modules["pyflink.common"] = fake_common
sys.modules["pyflink.common.serialization"] = fake_serialization


# --------------------------------------------------
# IMPORT DEL NOSTRO CODICE
# --------------------------------------------------

sys.path.append("flink/jobs")

from telemetry_job import TelemetryProcessor


# --------------------------------------------------
# TEST open()
# --------------------------------------------------

def test_open_apre_connessione_postgresql():

    fake_connection = MagicMock()
    fake_cursor = MagicMock()

    fake_connection.cursor.return_value = fake_cursor

    with patch("telemetry_job.psycopg2.connect") as mock_connect:

        mock_connect.return_value = fake_connection

        processor = TelemetryProcessor()

        processor.open(None)

        mock_connect.assert_called_once_with(
            host="postgres",
            port=5432,
            database="busdb",
            user="bususer",
            password="buspassword"
        )

        fake_connection.cursor.assert_called_once()

        assert processor.connection == fake_connection
        assert processor.cursor == fake_cursor


def test_save_telemetry_esegue_insert():

    processor = TelemetryProcessor()

    processor.cursor = MagicMock()

    data = {
        "event_id": "550e8400-e29b-41d4-a716-446655440020",
        "bus_id": "BUS_TEST",
        "timestamp": "2026-08-28T14:00:00+00:00",
        "latitude": 45.4642,
        "longitude": 9.1900,
        "speed": 50.0,
        "engine_on": True,
        "engine_temperature": 80.0,
        "fuel_level": 75.0,
        "passengers": 20
    }

    processor.save_telemetry(data)

    processor.cursor.execute.assert_called_once()

    args = processor.cursor.execute.call_args

    values = args[0][1]

    assert values == (
        "550e8400-e29b-41d4-a716-446655440020",
        "BUS_TEST",
        "2026-08-28T14:00:00+00:00",
        45.4642,
        9.1900,
        50.0,
        True,
        80.0,
        75.0,
        20
    )


def test_save_alert_esegue_insert():

    processor = TelemetryProcessor()

    processor.cursor = MagicMock()

    alert = {
        "event_id": "550e8400-e29b-41d4-a716-446655440021",
        "bus_id": "BUS_TEST",
        "alert_type": "OVERSPEED",
        "value": 120.0,
        "threshold": 90.0,
        "timestamp": "2026-08-28T14:00:00+00:00"
    }

    processor.save_alert(alert)

    processor.cursor.execute.assert_called_once()

    args = processor.cursor.execute.call_args

    values = args[0][1]

    assert values == (
        "550e8400-e29b-41d4-a716-446655440021",
        "BUS_TEST",
        "OVERSPEED",
        120.0,
        90.0,
        "2026-08-28T14:00:00+00:00"
    )


def test_process_element_genera_e_salva_alert():

    processor = TelemetryProcessor()

    processor.connection = MagicMock()
    processor.cursor = MagicMock()

    message = """
    {
        "event_id": "550e8400-e29b-41d4-a716-446655440022",
        "bus_id": "BUS_TEST",
        "timestamp": "2026-08-28T14:00:00+00:00",
        "latitude": 45.4642,
        "longitude": 9.1900,
        "speed": 120.0,
        "engine_on": true,
        "engine_temperature": 110.0,
        "fuel_level": 75.0,
        "passengers": 20
    }
    """

    risultati = list(
        processor.process_element(message, None)
    )

    # Verifica che la telemetria sia stata salvata
    processor.cursor.execute.assert_called()

    # Verifica che siano stati generati 2 alert
    assert len(risultati) == 2

    # Verifica i tipi di alert restituiti
    alert_types = [
        __import__("json").loads(alert)["alert_type"]
        for alert in risultati
    ]

    assert "OVERSPEED" in alert_types
    assert "ENGINE_OVERHEATING" in alert_types

    # Verifica che gli alert siano stati salvati
    assert processor.cursor.execute.call_count == 3

    # Verifica il commit
    processor.connection.commit.assert_called_once()


def test_close_chiude_connessione_e_cursor():

    processor = TelemetryProcessor()

    processor.cursor = MagicMock()
    processor.connection = MagicMock()

    processor.close()

    processor.cursor.close.assert_called_once()
    processor.connection.close.assert_called_once()


def test_process_element_errore_esegue_rollback():

    processor = TelemetryProcessor()

    processor.connection = MagicMock()
    processor.cursor = MagicMock()

    # Simuliamo un errore durante il salvataggio
    processor.save_telemetry = MagicMock(
        side_effect=Exception("Errore database")
    )

    message = """
    {
        "event_id": "550e8400-e29b-41d4-a716-446655440023",
        "bus_id": "BUS_TEST",
        "timestamp": "2026-08-28T14:00:00+00:00",
        "latitude": 45.4642,
        "longitude": 9.1900,
        "speed": 50.0,
        "engine_on": true,
        "engine_temperature": 80.0,
        "fuel_level": 75.0,
        "passengers": 20
    }
    """

    risultati = list(
        processor.process_element(message, None)
    )

    # Non devono essere prodotti alert
    assert risultati == []

    # In caso di errore deve essere eseguito rollback
    processor.connection.rollback.assert_called_once()

    # Il commit non deve essere eseguito
    processor.connection.commit.assert_not_called()
