import sys

sys.path.append("flink/jobs")

from alert_logic import check_alerts


def test_telemetria_normale_non_genera_alert():
    data = {
        "event_id": "550e8400-e29b-41d4-a716-446655440010",
        "bus_id": "BUS_TEST",
        "speed": 50.0,
        "engine_temperature": 80.0,
        "timestamp": "2026-08-28T14:00:00+00:00"
    }

    alerts = check_alerts(data)

    assert alerts == []


def test_overspeed_genera_alert():
    data = {
        "event_id": "550e8400-e29b-41d4-a716-446655440011",
        "bus_id": "BUS_TEST",
        "speed": 120.0,
        "engine_temperature": 80.0,
        "timestamp": "2026-08-28T14:00:00+00:00"
    }

    alerts = check_alerts(data)

    assert len(alerts) == 1
    assert alerts[0]["alert_type"] == "OVERSPEED"
    assert alerts[0]["value"] == 120.0
    assert alerts[0]["threshold"] == 90.0


def test_surriscaldamento_genera_alert():
    data = {
        "event_id": "550e8400-e29b-41d4-a716-446655440012",
        "bus_id": "BUS_TEST",
        "speed": 50.0,
        "engine_temperature": 110.0,
        "timestamp": "2026-08-28T14:00:00+00:00"
    }

    alerts = check_alerts(data)

    assert len(alerts) == 1
    assert alerts[0]["alert_type"] == "ENGINE_OVERHEATING"
    assert alerts[0]["value"] == 110.0
    assert alerts[0]["threshold"] == 100.0


def test_telemetria_genera_due_alert():
    data = {
        "event_id": "550e8400-e29b-41d4-a716-446655440013",
        "bus_id": "BUS_TEST",
        "speed": 120.0,
        "engine_temperature": 110.0,
        "timestamp": "2026-08-28T14:00:00+00:00"
    }

    alerts = check_alerts(data)

    assert len(alerts) == 2
    assert alerts[0]["alert_type"] == "OVERSPEED"
    assert alerts[1]["alert_type"] == "ENGINE_OVERHEATING"


def test_valori_esattamente_sulla_soglia_non_generano_alert():
    data = {
        "event_id": "550e8400-e29b-41d4-a716-446655440014",
        "bus_id": "BUS_TEST",
        "speed": 90.0,
        "engine_temperature": 100.0,
        "timestamp": "2026-08-28T14:00:00+00:00"
    }

    alerts = check_alerts(data)

    assert alerts == []


def test_velocita_appena_sopra_la_soglia_genera_alert():
    data = {
        "event_id": "550e8400-e29b-41d4-a716-446655440015",
        "bus_id": "BUS_TEST",
        "speed": 90.1,
        "engine_temperature": 80.0,
        "timestamp": "2026-08-28T14:00:00+00:00"
    }

    alerts = check_alerts(data)

    assert len(alerts) == 1
    assert alerts[0]["alert_type"] == "OVERSPEED"


def test_temperatura_appena_sopra_la_soglia_genera_alert():
    data = {
        "event_id": "550e8400-e29b-41d4-a716-446655440016",
        "bus_id": "BUS_TEST",
        "speed": 50.0,
        "engine_temperature": 100.1,
        "timestamp": "2026-08-28T14:00:00+00:00"
    }

    alerts = check_alerts(data)

    assert len(alerts) == 1
    assert alerts[0]["alert_type"] == "ENGINE_OVERHEATING"


def test_alert_contiene_tutti_i_dati_corretti():
    data = {
        "event_id": "550e8400-e29b-41d4-a716-446655440017",
        "bus_id": "BUS_TEST",
        "speed": 120.0,
        "engine_temperature": 80.0,
        "timestamp": "2026-08-28T14:00:00+00:00"
    }

    alerts = check_alerts(data)

    assert alerts == [{
        "event_id": "550e8400-e29b-41d4-a716-446655440017",
        "bus_id": "BUS_TEST",
        "alert_type": "OVERSPEED",
        "value": 120.0,
        "threshold": 90.0,
        "timestamp": "2026-08-28T14:00:00+00:00"
    }]
