-- Scenario 2: Tumbling 1-second window aggregation per device
-- Equivalent VPL: .partition_by(device_id).window(1s).aggregate(...)

DROP VIEW IF EXISTS mv_agg;
DROP STREAM IF EXISTS device_agg;
DROP STREAM IF EXISTS readings;

CREATE STREAM readings (
    ts int64,
    device_id string,
    temperature float
);

CREATE STREAM device_agg (
    win_start datetime64(3),
    device_id string,
    s float,
    a float,
    mn float,
    mx float
);

-- We use to_datetime64 to convert the int64 unix-millis ts column into a
-- datetime64 column that tumble() can window over.
CREATE MATERIALIZED VIEW mv_agg INTO device_agg AS
SELECT
    window_start AS win_start,
    device_id,
    sum(temperature) AS s,
    avg(temperature) AS a,
    min(temperature) AS mn,
    max(temperature) AS mx
FROM tumble(readings, to_datetime64(ts/1000.0, 3), 1s)
GROUP BY window_start, device_id;
