-- Scenario 3 (Arroyo): Stream-stream interval join on symbol, 5-second window.
-- Equivalent VPL:
--   stream Enriched = join(Trades, Quotes)
--       .on(Trades.symbol == Quotes.symbol)
--       .window(5s)
--       .select(symbol, trade_price, bid, ask)

CREATE TABLE trades (
    ts BIGINT,
    symbol TEXT,
    price DOUBLE,
    volume BIGINT,
    event_time TIMESTAMP GENERATED ALWAYS AS (TO_TIMESTAMP_MILLIS(ts)),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    connector = 'kafka',
    type = 'source',
    bootstrap_servers = 'redpanda:9092',
    topic = 'scenario-03-join',
    format = 'json',
    'source.offset' = 'earliest',
    'json.include_schema' = 'false',
    'value.subject' = 'Trade'
);

CREATE TABLE quotes (
    ts BIGINT,
    symbol TEXT,
    bid DOUBLE,
    ask DOUBLE,
    event_time TIMESTAMP GENERATED ALWAYS AS (TO_TIMESTAMP_MILLIS(ts)),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    connector = 'kafka',
    type = 'source',
    bootstrap_servers = 'redpanda:9092',
    topic = 'scenario-03-join',
    format = 'json',
    'source.offset' = 'earliest',
    'json.include_schema' = 'false',
    'value.subject' = 'Quote'
);

CREATE TABLE joined (
    symbol TEXT,
    trade_price DOUBLE,
    bid DOUBLE,
    ask DOUBLE
) WITH (
    connector = 'kafka',
    type = 'sink',
    bootstrap_servers = 'redpanda:9092',
    topic = 'scenario-03-join-out',
    format = 'json'
);

INSERT INTO joined
SELECT
    t.symbol,
    t.price AS trade_price,
    q.bid,
    q.ask
FROM trades t JOIN quotes q
    ON t.symbol = q.symbol
    AND t.event_time BETWEEN q.event_time - INTERVAL '5' SECOND
                          AND q.event_time + INTERVAL '5' SECOND;
