# Flink vs Varpulis Benchmark Test Harness

This test harness provides a fair comparison between Flink CEP and Varpulis CEP using MQTT as the common transport layer for both input and output.

## Architecture

```
                    MQTT Broker (mosquitto)
                           |
         +-----------------+-----------------+
         |                 |                 |
    Input Topics     Varpulis CEP       Flink CEP
  benchmark/input/#        |                 |
         |                 v                 v
         |        benchmark/output/    benchmark/output/
         |           varpulis              flink
         |                 |                 |
         +-----------------+-----------------+
                           |
                    Benchmark Script
                  (benchmark_mqtt.py)
```

## Prerequisites

1. **MQTT Broker** (e.g., Mosquitto)
   ```bash
   # Ubuntu/Debian
   sudo apt install mosquitto
   sudo systemctl start mosquitto

   # macOS
   brew install mosquitto
   brew services start mosquitto
   ```

2. **Python 3** with paho-mqtt
   ```bash
   pip install paho-mqtt
   ```

3. **Java 11+** and Maven (for Flink)
   ```bash
   # Check Java version
   java -version

   # Install Maven if needed
   sudo apt install maven  # or brew install maven
   ```

4. **Rust** (for Varpulis)
   ```bash
   # Should already be installed if you're working on Varpulis
   cargo --version
   ```

## Building

### Varpulis
```bash
cd /path/to/cep
cargo build --release
```

### Flink Benchmark
```bash
cd benchmarks/flink-comparison/test-harness
mvn compile
```

## Running the Benchmark

### Step 1: Start MQTT Broker
Make sure mosquitto (or another MQTT broker) is running on localhost:1883.

### Step 2: Start Varpulis
```bash
cd /path/to/cep
cargo run --release --features mqtt -- --run benchmarks/flink-comparison/test-harness/scenario2_mqtt.vpl
```

### Step 3: Start Flink (in another terminal)
```bash
cd benchmarks/flink-comparison/test-harness
mvn exec:java -Dexec.mainClass="com.benchmark.flink.Scenario2FlinkMqtt"
```

### Step 4: Run Benchmark (in another terminal)
```bash
cd benchmarks/flink-comparison/test-harness
python benchmark_mqtt.py --scenario scenario2
```

## Test Scenario 2: Login -> FailedTransaction

This scenario tests the temporal pattern matching capability:
- **Pattern**: Login event followed by a failed Transaction from the same user within 10 minutes
- **Events**: 10 events (4 Logins, 6 Transactions)
- **Expected Alerts**: 4 alerts (user1, user2, user3, user4)

### Expected Results

| User   | Amount   | Severity | Reason                              |
|--------|----------|----------|-------------------------------------|
| user1  | $500     | medium   | Login -> Failed TX (amount <= 1000) |
| user2  | $1,500   | high     | Login -> Failed TX (amount > 1000)  |
| user3  | $200     | medium   | Login -> Failed TX (amount <= 1000) |
| user4  | $2,000   | high     | Login -> Failed TX (amount > 1000)  |

### Non-matches

- user3's first transaction ($100, success) - not failed
- user4's first transaction ($50, success) - not failed

## Benchmark Output

The benchmark script will show:

1. **Results**: Alerts received from both systems
2. **Validation**: Whether each system produced the expected alerts
3. **Comparison**: Whether both systems produced identical results
4. **Performance**: Latency measurements for each alert

## Files

| File | Description |
|------|-------------|
| `scenario2_mqtt.vpl` | Varpulis pattern definition with MQTT config |
| `src/main/java/.../Scenario2FlinkMqtt.java` | Flink implementation with MQTT |
| `benchmark_mqtt.py` | Python script to run the benchmark |
| `pom.xml` | Maven build file for Flink |
| `data/scenario2_events.json` | Test data (for reference) |
| `data/scenario2_events.evt` | Test data in Varpulis event format |

## Code Comparison

### Varpulis (scenario2_mqtt.vpl) - 15 lines of pattern logic
```vpl
stream SuspiciousActivity = Login as login
    -> Transaction where user_id == login.user_id and status == "failed" as failed_tx
    .within(10m)
    .emit(
        alert_type: "LOGIN_THEN_FAILED_TX",
        user_id: login.user_id,
        severity: if failed_tx.amount > 1000 then "high" else "medium"
    )
```

### Flink (Scenario2FlinkMqtt.java) - ~80 lines of pattern logic
```java
Pattern<UserEvent, ?> pattern = Pattern.<UserEvent>begin("login")
    .where(new SimpleCondition<UserEvent>() {
        @Override
        public boolean filter(UserEvent event) {
            return "Login".equals(event.type);
        }
    })
    .followedBy("failedTx")
    .where(new IterativeCondition<UserEvent>() {
        @Override
        public boolean filter(UserEvent event, Context<UserEvent> ctx) throws Exception {
            if (!"Transaction".equals(event.type)) return false;
            if (!"failed".equals(event.status)) return false;
            for (UserEvent login : ctx.getEventsForPattern("login")) {
                if (login.userId.equals(event.userId)) return true;
            }
            return false;
        }
    })
    .within(Time.minutes(10));
```

## Troubleshooting

### MQTT Connection Refused
- Make sure mosquitto is running: `sudo systemctl status mosquitto`
- Check port 1883 is available: `netstat -an | grep 1883`

### Flink ClassNotFoundException
- Run `mvn compile` first
- Make sure you're in the test-harness directory

### No Alerts Received
- Check that both Varpulis and Flink are connected to MQTT
- Verify topics are correct: `mosquitto_sub -t "#" -v`
