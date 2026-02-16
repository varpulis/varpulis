//! Hover documentation provider

#![allow(clippy::useless_format)]

use tower_lsp::lsp_types::{Hover, HoverContents, MarkupContent, MarkupKind, Position};

/// Get hover documentation for a position in the document
pub fn get_hover(text: &str, position: Position) -> Option<Hover> {
    let word = get_word_at_position(text, position)?;

    let docs = get_documentation(&word)?;

    Some(Hover {
        contents: HoverContents::Markup(MarkupContent {
            kind: MarkupKind::Markdown,
            value: docs,
        }),
        range: None,
    })
}

/// Extract the word at the given position
fn get_word_at_position(text: &str, position: Position) -> Option<String> {
    let lines: Vec<&str> = text.lines().collect();
    let line = lines.get(position.line as usize)?;
    let col = position.character as usize;

    if col > line.len() {
        return None;
    }

    // Find word boundaries
    let chars: Vec<char> = line.chars().collect();

    // Find start of word
    let mut start = col;
    while start > 0 && is_word_char(chars.get(start - 1).copied()) {
        start -= 1;
    }

    // Find end of word
    let mut end = col;
    while end < chars.len() && is_word_char(chars.get(end).copied()) {
        end += 1;
    }

    if start == end {
        return None;
    }

    Some(chars[start..end].iter().collect())
}

fn is_word_char(c: Option<char>) -> bool {
    c.map(|c| c.is_alphanumeric() || c == '_').unwrap_or(false)
}

/// Get documentation for a keyword, function, or type
fn get_documentation(word: &str) -> Option<String> {
    match word {
        // Keywords - Declarations
        "stream" => Some(format!(
            "## stream\n\n\
            Declares a data stream from an external source.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            stream <Name> from \"<source_uri>\"\n\
            ```\n\n\
            **Example:**\n\
            ```vpl\n\
            stream SensorData from \"mqtt://localhost:1883/sensors\"\n\
                .where(temperature > 0)\n\
                .emit()\n\
            ```"
        )),

        "event" => Some(format!(
            "## event\n\n\
            Declares an event type with typed fields.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            event <Name> {{\n\
                field1: type1,\n\
                field2: type2\n\
            }}\n\
            ```\n\n\
            **Example:**\n\
            ```vpl\n\
            event TemperatureReading {{\n\
                sensor_id: str,\n\
                temperature: float,\n\
                timestamp: timestamp\n\
            }}\n\
            ```"
        )),

        "pattern" => Some(format!(
            "## pattern\n\n\
            Declares a SASE+ pattern for complex event processing.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            pattern <Name> = SEQ(<events>) within <duration>\n\
            ```\n\n\
            **Example:**\n\
            ```vpl\n\
            pattern TemperatureSpike = SEQ(a: TempReading, b: TempReading)\n\
                where b.temp > a.temp + 10\n\
                within 5m\n\
            ```"
        )),

        "from" => Some(format!(
            "## .from()\n\n\
            Binds a stream to a connector source.\n\n\
            **Syntax:** `EventType.from(Connector, key: value, ...)`\n\n\
            **Example:**\n\
            ```vpl\n\
            connector MqttSensors = mqtt(url: \"localhost:1883\")\n\
            stream Temperatures = TemperatureReading.from(MqttSensors, topic: \"sensors/#\")\n\
            ```"
        )),

        // Control flow
        "if" => Some(format!(
            "## if\n\n\
            Conditional expression.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            if condition then\n\
                expr\n\
            elif condition then\n\
                expr\n\
            else\n\
                expr\n\
            ```"
        )),

        "match" => Some(format!(
            "## match\n\n\
            Pattern matching expression.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            match value {{\n\
                pattern1 => result1,\n\
                pattern2 => result2,\n\
                _ => default\n\
            }}\n\
            ```"
        )),

        // Variables
        "let" => Some(format!(
            "## let\n\n\
            Declares an immutable variable binding.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            let name = value\n\
            let name: Type = value\n\
            ```"
        )),

        "var" => Some(format!(
            "## var\n\n\
            Declares a mutable variable.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            var name = value\n\
            var name: Type = value\n\
            ```"
        )),

        "const" => Some(format!(
            "## const\n\n\
            Declares a compile-time constant.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            const NAME = value\n\
            ```"
        )),

        "fn" => Some(format!(
            "## fn\n\n\
            Declares a function.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            fn name(param1: Type1, param2: Type2) -> ReturnType {{\n\
                body\n\
            }}\n\
            ```"
        )),

        // Stream operations
        "where" => Some(format!(
            "## .where()\n\n\
            Filters stream events based on a condition.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            stream.where(condition)\n\
            ```\n\n\
            **Example:**\n\
            ```vpl\n\
            stream SensorData from \"mqtt://...\"\n\
                .where(temperature > 25 and humidity < 80)\n\
            ```"
        )),

        "select" => Some(format!(
            "## .select()\n\n\
            Projects/transforms stream events to a new shape.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            stream.select({{ field1: expr1, field2: expr2 }})\n\
            ```\n\n\
            **Example:**\n\
            ```vpl\n\
            stream.select({{\n\
                celsius: (fahrenheit - 32) * 5/9,\n\
                source: sensor_id\n\
            }})\n\
            ```"
        )),

        "aggregate" => Some(format!(
            "## .aggregate()\n\n\
            Aggregates events within a window.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            stream.aggregate({{ field: agg_func(expr) }})\n\
            ```\n\n\
            **Example:**\n\
            ```vpl\n\
            stream\n\
                .window(tumbling(5m))\n\
                .aggregate({{\n\
                    avg_temp: avg(temperature),\n\
                    max_temp: max(temperature),\n\
                    count: count()\n\
                }})\n\
            ```"
        )),

        "window" => Some(format!(
            "## .window()\n\n\
            Defines a time window for aggregation.\n\n\
            **Window types:**\n\
            - `tumbling(duration)` - Non-overlapping fixed windows\n\
            - `sliding(size, slide)` - Overlapping windows\n\
            - `session_window(gap)` - Gap-based sessions\n\
            **Example:**\n\
            ```vpl\n\
            stream\n\
                .window(sliding(10m, 1m))\n\
                .aggregate({{ avg: avg(value) }})\n\
            ```"
        )),

        "emit" => Some(format!(
            "## .emit()\n\n\
            Outputs the stream to the console or default sink.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            stream.emit()\n\
            ```"
        )),

        "to" => Some(format!(
            "## .to()\n\n\
            Outputs the stream to a specified sink.\n\n\
            **Supported sinks:**\n\
            - `console://` - Console output\n\
            - `file://` - File output\n\
            - `mqtt://` - MQTT publish\n\
            - `kafka://` - Kafka produce\n\
            - `http://` - HTTP POST\n\n\
            **Example:**\n\
            ```vpl\n\
            stream.to(\"file://output.json\")\n\
            ```"
        )),

        "forecast" => Some(format!(
            "## .forecast()\n\n\
            PST-based pattern forecasting. Predicts whether a partially-matched\n\
            sequence pattern will complete, and estimates when.\n\n\
            **Parameters:**\n\
            - `mode` — Preset: `\"fast\"`, `\"accurate\"`, or `\"balanced\"` (default)\n\
            - `confidence` — Minimum probability to emit (default 0.5)\n\
            - `horizon` — Forecast window (default = within duration)\n\
            - `warmup` — Min events before forecasting starts (default 100)\n\
            - `max_depth` — PST context depth (default 3)\n\
            - `hawkes` — Enable Hawkes intensity modulation (default true)\n\
            - `conformal` — Enable conformal prediction intervals (default true)\n\n\
            **Modes:**\n\
            - `\"fast\"` — hawkes:off, conformal:off, warmup:50, max_depth:3\n\
            - `\"accurate\"` — hawkes:on, conformal:on, warmup:200, max_depth:5\n\
            - `\"balanced\"` — hawkes:on, conformal:on, warmup:100 (default)\n\n\
            **Built-in variables** (after `.forecast()`):\n\
            - `forecast_probability` — Completion probability (0.0–1.0)\n\
            - `forecast_confidence` — Prediction stability (0.0–1.0)\n\
            - `forecast_time` — Expected time to completion (ns)\n\
            - `forecast_state` — Current NFA state label\n\
            - `forecast_context_depth` — PST context depth used\n\
            - `forecast_lower` — Conformal prediction interval lower bound\n\
            - `forecast_upper` — Conformal prediction interval upper bound\n\n\
            **Example:**\n\
            ```vpl\n\
            stream Alerts = Event as e1\n\
                -> Event as e2 where e2.value > e1.value\n\
                .within(5m)\n\
                .forecast(mode: \"accurate\")\n\
                .where(forecast_confidence > 0.8)\n\
                .emit(prob: forecast_probability)\n\
            ```"
        )),

        "enrich" => Some(format!(
            "## .enrich()\n\n\
            Enriches streaming events with data from an external connector\n\
            (HTTP API, SQL database, or Redis).\n\n\
            **Parameters:**\n\
            - `connector` — Connector name (first positional arg)\n\
            - `key` — Expression to use as lookup key (required)\n\
            - `fields` — List of fields to extract from the response (required)\n\
            - `cache_ttl` — How long to cache results (optional, e.g. `5m`)\n\
            - `timeout` — Max time to wait for response (optional, default `5s`)\n\
            - `fallback` — Default value on failure (optional)\n\n\
            **Built-in variables** (after `.enrich()`):\n\
            - `enrich_status` — `\"ok\"`, `\"error\"`, `\"cached\"`, or `\"timeout\"`\n\
            - `enrich_latency_ms` — Lookup latency in ms (0 for cache hits)\n\n\
            **Example:**\n\
            ```vpl\n\
            connector WeatherAPI = http(url: \"https://api.weather.com/v1\")\n\n\
            stream Enriched = Temperature as t\n\
                .enrich(WeatherAPI, key: t.city, fields: [forecast, humidity], cache_ttl: 5m)\n\
                .where(forecast == \"rain\")\n\
                .emit(city: t.city, forecast: forecast)\n\
            ```"
        )),

        "partition_by" => Some(format!(
            "## .partition_by()\n\n\
            Partitions the stream by a key for parallel processing.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            stream.partition_by(key_expr)\n\
            ```\n\n\
            **Example:**\n\
            ```vpl\n\
            stream\n\
                .partition_by(sensor_id)\n\
                .window(tumbling(1m))\n\
                .aggregate({{ avg: avg(value) }})\n\
            ```"
        )),

        "join" => Some(format!(
            "## .join()\n\n\
            Joins two streams based on a condition.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            stream1.join(stream2).on(condition)\n\
            ```\n\n\
            **Example:**\n\
            ```vpl\n\
            temperatures\n\
                .join(humidity)\n\
                .on(t.sensor_id == h.sensor_id)\n\
                .select({{ ... }})\n\
            ```"
        )),

        // SASE+ Pattern operators
        "SEQ" => Some(format!(
            "## SEQ (Sequence)\n\n\
            Matches events occurring in sequence.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            SEQ(a: EventA, b: EventB, c: EventC)\n\
            ```\n\n\
            Events must occur in the specified order within the time window."
        )),

        "AND" => Some(format!(
            "## AND (Conjunction)\n\n\
            Matches when all specified events occur (any order).\n\n\
            **Syntax:**\n\
            ```vpl\n\
            AND(a: EventA, b: EventB)\n\
            ```\n\n\
            Both events must occur within the time window."
        )),

        "OR" => Some(format!(
            "## OR (Disjunction)\n\n\
            Matches when any of the specified events occur.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            OR(a: EventA, b: EventB)\n\
            ```"
        )),

        "NOT" => Some(format!(
            "## NOT (Negation)\n\n\
            Matches when an event does NOT occur.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            SEQ(a: EventA, NOT(EventB), c: EventC)\n\
            ```\n\n\
            EventB must not occur between EventA and EventC."
        )),

        "within" => Some(format!(
            "## within\n\n\
            Specifies the time window for pattern matching.\n\n\
            **Syntax:**\n\
            ```vpl\n\
            pattern ... within <duration>\n\
            ```\n\n\
            **Duration units:**\n\
            - `ms` - milliseconds\n\
            - `s` - seconds\n\
            - `m` - minutes\n\
            - `h` - hours\n\
            - `d` - days\n\n\
            **Example:**\n\
            ```vpl\n\
            pattern Alert = SEQ(a: Warning, b: Error) within 30s\n\
            ```"
        )),

        // Aggregation functions
        "sum" => Some(format!(
            "## sum(expr)\n\n\
            Returns the sum of values in a window.\n\n\
            **Example:**\n\
            ```vpl\n\
            .aggregate({{ total: sum(amount) }})\n\
            ```"
        )),

        "avg" => Some(format!(
            "## avg(expr)\n\n\
            Returns the average of values in a window.\n\n\
            **Example:**\n\
            ```vpl\n\
            .aggregate({{ average: avg(temperature) }})\n\
            ```"
        )),

        "count" => Some(format!(
            "## count()\n\n\
            Returns the count of events in a window.\n\n\
            **Example:**\n\
            ```vpl\n\
            .aggregate({{ total_events: count() }})\n\
            ```"
        )),

        "min" => Some(format!(
            "## min(expr)\n\n\
            Returns the minimum value in a window.\n\n\
            **Example:**\n\
            ```vpl\n\
            .aggregate({{ lowest: min(price) }})\n\
            ```"
        )),

        "max" => Some(format!(
            "## max(expr)\n\n\
            Returns the maximum value in a window.\n\n\
            **Example:**\n\
            ```vpl\n\
            .aggregate({{ highest: max(price) }})\n\
            ```"
        )),

        "stddev" => Some(format!(
            "## stddev(expr)\n\n\
            Returns the standard deviation of values in a window.\n\n\
            **Example:**\n\
            ```vpl\n\
            .aggregate({{ std: stddev(value) }})\n\
            ```"
        )),

        "variance" => Some(format!(
            "## variance(expr)\n\n\
            Returns the variance of values in a window.\n\n\
            **Example:**\n\
            ```vpl\n\
            .aggregate({{ var: variance(value) }})\n\
            ```"
        )),

        "first" => Some(format!(
            "## first(expr)\n\n\
            Returns the first value in a window.\n\n\
            **Example:**\n\
            ```vpl\n\
            .aggregate({{ earliest: first(timestamp) }})\n\
            ```"
        )),

        "last" => Some(format!(
            "## last(expr)\n\n\
            Returns the last value in a window.\n\n\
            **Example:**\n\
            ```vpl\n\
            .aggregate({{ latest: last(value) }})\n\
            ```"
        )),

        "collect" => Some(format!(
            "## collect(expr)\n\n\
            Collects all values in a window into a list.\n\n\
            **Example:**\n\
            ```vpl\n\
            .aggregate({{ all_values: collect(value) }})\n\
            ```"
        )),

        "distinct" => Some(format!(
            "## distinct(expr) / .distinct()\n\n\
            Returns distinct values or removes duplicates from a stream.\n\n\
            **As aggregation:**\n\
            ```vpl\n\
            .aggregate({{ unique: distinct(category) }})\n\
            ```\n\n\
            **As stream operation:**\n\
            ```vpl\n\
            stream.distinct()\n\
            ```"
        )),

        // Window functions
        "tumbling" => Some(format!(
            "## tumbling(duration)\n\n\
            Creates non-overlapping fixed-size windows.\n\n\
            **Example:**\n\
            ```vpl\n\
            stream\n\
                .window(tumbling(5m))\n\
                .aggregate({{ count: count() }})\n\
            ```\n\n\
            Events are grouped into consecutive, non-overlapping time buckets."
        )),

        "sliding" => Some(format!(
            "## sliding(size, slide)\n\n\
            Creates overlapping sliding windows.\n\n\
            **Example:**\n\
            ```vpl\n\
            stream\n\
                .window(sliding(10m, 1m))\n\
                .aggregate({{ avg: avg(value) }})\n\
            ```\n\n\
            - `size`: Window duration\n\
            - `slide`: How often to emit results"
        )),

        "session_window" => Some(format!(
            "## session_window(gap)\n\n\
            Creates session windows based on inactivity gaps.\n\n\
            **Example:**\n\
            ```vpl\n\
            stream\n\
                .window(session_window(30s))\n\
                .aggregate({{ ... }})\n\
            ```\n\n\
            A new window starts after `gap` duration of inactivity."
        )),

        // Built-in functions
        "now" => Some(format!(
            "## now()\n\n\
            Returns the current timestamp.\n\n\
            **Example:**\n\
            ```vpl\n\
            let current_time = now()\n\
            ```"
        )),

        "len" => Some(format!(
            "## len(collection)\n\n\
            Returns the length of a string or collection.\n\n\
            **Example:**\n\
            ```vpl\n\
            let size = len(items)\n\
            let str_len = len(\"hello\")\n\
            ```"
        )),

        "abs" => Some(format!(
            "## abs(number)\n\n\
            Returns the absolute value.\n\n\
            **Example:**\n\
            ```vpl\n\
            let positive = abs(-5)  # Returns 5\n\
            ```"
        )),

        "sqrt" => Some(format!(
            "## sqrt(number)\n\n\
            Returns the square root.\n\n\
            **Example:**\n\
            ```vpl\n\
            let root = sqrt(16)  # Returns 4.0\n\
            ```"
        )),

        "pow" => Some(format!(
            "## pow(base, exponent)\n\n\
            Returns base raised to the power of exponent.\n\n\
            **Example:**\n\
            ```vpl\n\
            let squared = pow(2, 3)  # Returns 8\n\
            ```"
        )),

        "floor" => Some(format!(
            "## floor(number)\n\n\
            Rounds down to the nearest integer.\n\n\
            **Example:**\n\
            ```vpl\n\
            let floored = floor(3.7)  # Returns 3\n\
            ```"
        )),

        "ceil" => Some(format!(
            "## ceil(number)\n\n\
            Rounds up to the nearest integer.\n\n\
            **Example:**\n\
            ```vpl\n\
            let ceiled = ceil(3.2)  # Returns 4\n\
            ```"
        )),

        "round" => Some(format!(
            "## round(number)\n\n\
            Rounds to the nearest integer.\n\n\
            **Example:**\n\
            ```vpl\n\
            let rounded = round(3.5)  # Returns 4\n\
            ```"
        )),

        // Types
        "int" => Some(format!(
            "## int\n\n\
            64-bit signed integer type.\n\n\
            **Example:**\n\
            ```vpl\n\
            let count: int = 42\n\
            ```"
        )),

        "float" => Some(format!(
            "## float\n\n\
            64-bit floating-point number type.\n\n\
            **Example:**\n\
            ```vpl\n\
            let temperature: float = 23.5\n\
            ```"
        )),

        "bool" => Some(format!(
            "## bool\n\n\
            Boolean type (`true` or `false`).\n\n\
            **Example:**\n\
            ```vpl\n\
            let is_active: bool = true\n\
            ```"
        )),

        "str" => Some(format!(
            "## str\n\n\
            String type for text values.\n\n\
            **Example:**\n\
            ```vpl\n\
            let name: str = \"sensor-1\"\n\
            ```"
        )),

        "timestamp" => Some(format!(
            "## timestamp\n\n\
            Timestamp type for date/time values.\n\n\
            **Literal syntax:**\n\
            ```vpl\n\
            let t = @2024-01-01T12:00:00Z\n\
            ```\n\n\
            **Example:**\n\
            ```vpl\n\
            event Reading {{\n\
                ts: timestamp,\n\
                value: float\n\
            }}\n\
            ```"
        )),

        "duration" => Some(format!(
            "## duration\n\n\
            Duration type for time intervals.\n\n\
            **Units:**\n\
            - `ms` - milliseconds\n\
            - `s` - seconds\n\
            - `m` - minutes\n\
            - `h` - hours\n\
            - `d` - days\n\n\
            **Example:**\n\
            ```vpl\n\
            let timeout: duration = 30s\n\
            let window_size = 5m\n\
            ```"
        )),

        "Stream" => Some(format!(
            "## Stream<T>\n\n\
            A continuous flow of events of type T.\n\n\
            **Example:**\n\
            ```vpl\n\
            fn process(input: Stream<SensorData>) -> Stream<Alert> {{\n\
                input\n\
                    .where(value > threshold)\n\
                    .select({{ alert: \"High value\" }})\n\
            }}\n\
            ```"
        )),

        "list" => Some(format!(
            "## list\n\n\
            Dynamic list/array type.\n\n\
            **Example:**\n\
            ```vpl\n\
            let items: list = [1, 2, 3, 4, 5]\n\
            ```"
        )),

        "map" => Some(format!(
            "## map\n\n\
            Key-value map type, or the `map` function for transforming collections.\n\n\
            **As type:**\n\
            ```vpl\n\
            let data: map = {{ \"key\": \"value\" }}\n\
            ```\n\n\
            **As function:**\n\
            ```vpl\n\
            let doubled = map(items, |x| => x * 2)\n\
            ```"
        )),

        "filter" => Some(format!(
            "## filter(collection, predicate)\n\n\
            Filters a collection keeping only elements matching the predicate.\n\n\
            **Example:**\n\
            ```vpl\n\
            let positive = filter(numbers, |x| => x > 0)\n\
            ```"
        )),

        "reduce" => Some(format!(
            "## reduce(collection, initial, accumulator)\n\n\
            Reduces a collection to a single value.\n\n\
            **Example:**\n\
            ```vpl\n\
            let total = reduce(numbers, 0, |acc, x| => acc + x)\n\
            ```"
        )),

        // Logical operators
        "and" => Some(format!(
            "## and\n\n\
            Logical AND operator.\n\n\
            **Example:**\n\
            ```vpl\n\
            if temp > 25 and humidity > 80 then\n\
                \"Hot and humid\"\n\
            ```"
        )),

        "or" => Some(format!(
            "## or\n\n\
            Logical OR operator.\n\n\
            **Example:**\n\
            ```vpl\n\
            if status == \"error\" or status == \"critical\" then\n\
                emit_alert()\n\
            ```"
        )),

        "not" => Some(format!(
            "## not\n\n\
            Logical NOT operator.\n\n\
            **Example:**\n\
            ```vpl\n\
            if not is_valid then\n\
                handle_error()\n\
            ```"
        )),

        "true" => Some("## true\n\nBoolean true literal.".to_string()),
        "false" => Some("## false\n\nBoolean false literal.".to_string()),
        "null" => Some("## null\n\nNull/empty value.".to_string()),

        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_word_at_position() {
        let text = "stream SensorData from";
        let word = get_word_at_position(
            text,
            Position {
                line: 0,
                character: 0,
            },
        );
        assert_eq!(word, Some("stream".to_string()));

        let word = get_word_at_position(
            text,
            Position {
                line: 0,
                character: 7,
            },
        );
        assert_eq!(word, Some("SensorData".to_string()));
    }

    #[test]
    fn test_hover_stream() {
        let text = "stream SensorData from";
        let hover = get_hover(
            text,
            Position {
                line: 0,
                character: 0,
            },
        );
        assert!(hover.is_some());
    }
}
