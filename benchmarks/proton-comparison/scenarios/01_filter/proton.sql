-- Scenario 1: Filter — keep only ticks where price > 50
-- Equivalent VPL: stream X = Tick .where(price > 50)
--
-- Run via: cat 01_filter.sql | docker exec -i bench-proton proton client --multiquery

DROP VIEW IF EXISTS mv_filter;
DROP STREAM IF EXISTS ticks_filtered;
DROP STREAM IF EXISTS ticks;

CREATE STREAM ticks (
    ts int64,
    symbol string,
    price float,
    volume int32
);

CREATE STREAM ticks_filtered (
    symbol string,
    price float,
    volume int32
);

CREATE MATERIALIZED VIEW mv_filter INTO ticks_filtered AS
SELECT symbol, price, volume FROM ticks WHERE price > 50.0;
