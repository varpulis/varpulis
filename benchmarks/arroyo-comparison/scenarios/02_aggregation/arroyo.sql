-- Scenario 2 (Arroyo): Tumbling 1-second window aggregation per device
-- Equivalent VPL: .partition_by(device_id).window(1s).aggregate(...)

CREATE TABLE readings (
    ts BIGINT,
    device_id TEXT,
    temperature DOUBLE,
    event_time TIMESTAMP GENERATED ALWAYS AS (TO_TIMESTAMP_MILLIS(ts)),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    connector = 'kafka',
    type = 'source',
    bootstrap_servers = 'redpanda:9092',
    topic = 'scenario-02-agg',
    format = 'json',
    'source.offset' = 'earliest'
);

CREATE TABLE device_agg (
    win_start TIMESTAMP,
    device_id TEXT,
    s DOUBLE,
    a DOUBLE,
    mn DOUBLE,
    mx DOUBLE
) WITH (
    connector = 'kafka',
    type = 'sink',
    bootstrap_servers = 'redpanda:9092',
    topic = 'scenario-02-agg-out',
    format = 'json'
);

INSERT INTO device_agg
SELECT
    window.start AS win_start,
    device_id,
    SUM(temperature) AS s,
    AVG(temperature) AS a,
    MIN(temperature) AS mn,
    MAX(temperature) AS mx
FROM (
    SELECT TUMBLE(INTERVAL '1' SECOND) AS window, device_id, temperature
    FROM readings
)
GROUP BY window, device_id;
