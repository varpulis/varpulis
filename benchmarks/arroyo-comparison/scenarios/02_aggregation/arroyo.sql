-- Scenario 2 (Arroyo): Tumbling 1-second window aggregation per device
-- Equivalent VPL: .partition_by(device_id).window(1s).aggregate(...)

-- Arroyo 0.15.0 quirks worth noting for the comparison writeup:
--  1. WATERMARK is a comma-separated *column constraint*, not a top-level
--     clause. Lowercase `watermark FOR <col> as <expr>`.
--  2. The expression MUST reference physical columns (here: `ts`), NOT the
--     virtual GENERATED `event_time`, because watermark planning goes
--     through `physical_schema()` which excludes virtuals.
--  3. The expression must be NOT NULL — `ts BIGINT NOT NULL` + the
--     `to_timestamp_millis` builtin propagate non-null.
--  4. TUMBLE + aggregate MUST be at the SAME SELECT level as the
--     GROUP BY — a nested "inner projects tumble, outer aggregates"
--     shape is rejected with "can't call a windowing function without
--     grouping by it".
CREATE TABLE readings (
    ts BIGINT NOT NULL,
    device_id TEXT,
    temperature DOUBLE,
    -- DataFusion's nullability propagation for scalar time functions
    -- (`to_timestamp_millis`, `from_unixtime`, etc.) marks every result
    -- as nullable regardless of input. The only way to get a NOT NULL
    -- TIMESTAMP from a NOT NULL BIGINT — which Arroyo's WATERMARK FOR
    -- constraint check requires — is the direct `CAST(... as TIMESTAMP)`
    -- which Arroyo/DataFusion treats as "interpret the bigint as epoch
    -- seconds". Our generator emits epoch **milliseconds**, so we have
    -- to divide by 1000 first.
    event_time TIMESTAMP NOT NULL GENERATED ALWAYS AS (CAST(ts / 1000 as TIMESTAMP)),
    watermark FOR event_time as CAST(ts / 1000 as TIMESTAMP) - INTERVAL '1' SECOND
) WITH (
    connector = 'kafka',
    type = 'source',
    bootstrap_servers = 'redpanda:9092',
    topic = 'scenario-02-agg',
    format = 'json',
    'source.offset' = 'earliest'
);

CREATE TABLE device_agg WITH (
    connector = 'kafka',
    type = 'sink',
    bootstrap_servers = 'redpanda:9092',
    topic = 'scenario-02-agg-out',
    format = 'json'
);

INSERT INTO device_agg
SELECT window.end as win_end, device_id, avg_temp
FROM (
    SELECT tumble(interval '1' second) as window,
           device_id,
           avg(temperature) as avg_temp
    FROM readings
    GROUP BY device_id, window
);
