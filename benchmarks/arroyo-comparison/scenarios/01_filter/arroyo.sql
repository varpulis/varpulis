-- Scenario 1 (Arroyo): Filter ticks where price > 50, Kafka source + sink
-- Equivalent VPL: stream X = Tick .where(price > 50)

CREATE TABLE ticks (
    ts BIGINT,
    symbol TEXT,
    price DOUBLE,
    volume BIGINT
) WITH (
    connector = 'kafka',
    type = 'source',
    bootstrap_servers = 'redpanda:9092',
    topic = 'scenario-01-filter',
    format = 'json',
    'source.offset' = 'earliest'
);

CREATE TABLE ticks_filtered (
    symbol TEXT,
    price DOUBLE,
    volume BIGINT
) WITH (
    connector = 'kafka',
    type = 'sink',
    bootstrap_servers = 'redpanda:9092',
    topic = 'scenario-01-filter-out',
    format = 'json'
);

INSERT INTO ticks_filtered
SELECT symbol, price, volume FROM ticks WHERE price > 50.0;
