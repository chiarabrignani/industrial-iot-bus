CREATE TABLE telemetry (
    event_id UUID PRIMARY KEY,
    bus_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    speed DOUBLE PRECISION,
    engine_on BOOLEAN,
    engine_temperature DOUBLE PRECISION,
    fuel_level DOUBLE PRECISION,
    passengers INTEGER
);

CREATE TABLE alerts (
    event_id UUID NOT NULL,
    bus_id VARCHAR(50) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    value DOUBLE PRECISION,
    threshold DOUBLE PRECISION,
    timestamp TIMESTAMPTZ NOT NULL
);