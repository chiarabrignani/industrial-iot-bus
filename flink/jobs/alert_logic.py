def check_alerts(data):
    """
    Controlla una telemetria e restituisce
    gli eventuali alert.
    """

    alerts = []

    if data["speed"] > 90:
        alerts.append({
            "event_id": data["event_id"],
            "bus_id": data["bus_id"],
            "alert_type": "OVERSPEED",
            "value": data["speed"],
            "threshold": 90.0,
            "timestamp": data["timestamp"]
        })

    if data["engine_temperature"] > 100:
        alerts.append({
            "event_id": data["event_id"],
            "bus_id": data["bus_id"],
            "alert_type": "ENGINE_OVERHEATING",
            "value": data["engine_temperature"],
            "threshold": 100.0,
            "timestamp": data["timestamp"]
        })

    return alerts