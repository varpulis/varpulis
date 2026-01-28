package com.benchmark.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.time.Duration;
import java.util.*;

/**
 * Flink implementation of Scenario 3 with MQTT input/output.
 * Pattern: Suspicious Transaction -> 3+ Small Purchases -> Large Withdrawal
 * (Multi-event fraud detection)
 */
public class Scenario3FlinkMqtt {

    // MQTT Configuration
    private static final String MQTT_BROKER = "tcp://localhost:1883";
    private static final String INPUT_TOPIC = "benchmark/input/Transaction";
    private static final String OUTPUT_TOPIC = "benchmark/output/flink";

    // Transaction event
    public static class Transaction implements java.io.Serializable {
        public String userId;
        public double amount;
        public String type; // "purchase", "transfer", "withdrawal", "deposit"
        public String merchant;
        public String location;
        public double riskScore;
        public long timestamp;

        public boolean isSuspicious() {
            return riskScore > 0.7 || amount > 5000;
        }

        public boolean isSmallPurchase() {
            return amount < 100 && "purchase".equals(type);
        }

        public boolean isLargeWithdrawal() {
            return "withdrawal".equals(type) && amount > 1000;
        }

        @Override
        public String toString() {
            return String.format("Transaction{user=%s, type=%s, amount=%.2f, risk=%.2f, ts=%d}",
                    userId, type, amount, riskScore, timestamp);
        }
    }

    // Fraud alert output
    public static class FraudAlert implements java.io.Serializable {
        public String alertType;
        public String severity;
        public String userId;
        public double initialRiskScore;
        public double initialAmount;
        public double smallTxTotal;
        public double withdrawalAmount;
        public double totalDurationHours;
        public double confidence;
        public String recommendation;

        public String toJson() {
            return String.format(
                "{\"alert_type\":\"%s\",\"severity\":\"%s\",\"user_id\":\"%s\",\"initial_risk_score\":%.2f,\"initial_amount\":%.2f,\"small_tx_total\":%.2f,\"withdrawal_amount\":%.2f,\"total_duration_hours\":%.2f,\"confidence\":%.2f,\"recommendation\":\"%s\"}",
                alertType, severity, userId, initialRiskScore, initialAmount, smallTxTotal, withdrawalAmount, totalDurationHours, confidence, recommendation
            );
        }

        @Override
        public String toString() {
            return String.format("FraudAlert{user=%s, severity=%s, withdrawal=%.2f}",
                    userId, severity, withdrawalAmount);
        }
    }

    /**
     * MQTT Source Function - receives Transaction events from MQTT
     */
    public static class MqttSourceFunction implements SourceFunction<Transaction> {
        private volatile boolean isRunning = true;
        private MqttClient mqttClient;

        @Override
        public void run(SourceContext<Transaction> ctx) throws Exception {
            mqttClient = new MqttClient(MQTT_BROKER, "flink-scenario3-source-" + UUID.randomUUID(), new MemoryPersistence());

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            options.setAutomaticReconnect(true);

            ObjectMapper mapper = new ObjectMapper();

            mqttClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.err.println("[FLINK] MQTT connection lost: " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    try {
                        JsonNode node = mapper.readTree(message.getPayload());
                        Transaction event = parseTransaction(node);
                        if (event != null) {
                            synchronized (ctx.getCheckpointLock()) {
                                ctx.collectWithTimestamp(event, event.timestamp);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("[FLINK] Error parsing message: " + e.getMessage());
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {}
            });

            mqttClient.connect(options);
            System.out.println("[FLINK] Connected to MQTT broker: " + MQTT_BROKER);

            mqttClient.subscribe(INPUT_TOPIC, 1);
            System.out.println("[FLINK] Subscribed to: " + INPUT_TOPIC);

            // Keep running until cancelled
            while (isRunning && mqttClient.isConnected()) {
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
            try {
                if (mqttClient != null && mqttClient.isConnected()) {
                    mqttClient.disconnect();
                    mqttClient.close();
                }
            } catch (MqttException e) {
                System.err.println("[FLINK] Error disconnecting: " + e.getMessage());
            }
        }

        private Transaction parseTransaction(JsonNode node) {
            Transaction tx = new Transaction();
            tx.userId = node.has("user_id") ? node.get("user_id").asText() : "";
            tx.amount = node.has("amount") ? node.get("amount").asDouble() : 0.0;
            tx.type = node.has("type") ? node.get("type").asText() : "unknown";
            tx.merchant = node.has("merchant") ? node.get("merchant").asText() : "";
            tx.location = node.has("location") ? node.get("location").asText() : "";
            tx.riskScore = node.has("risk_score") ? node.get("risk_score").asDouble() : 0.0;
            tx.timestamp = node.has("ts") ? node.get("ts").asLong() : System.currentTimeMillis();
            return tx;
        }
    }

    /**
     * MQTT Sink Function - publishes fraud alerts to MQTT
     */
    public static class MqttSinkFunction implements SinkFunction<FraudAlert> {
        private transient MqttClient mqttClient;
        private transient boolean initialized = false;

        private void ensureInitialized() throws MqttException {
            if (!initialized) {
                mqttClient = new MqttClient(MQTT_BROKER, "flink-scenario3-sink-" + UUID.randomUUID(), new MemoryPersistence());
                MqttConnectOptions options = new MqttConnectOptions();
                options.setCleanSession(true);
                options.setAutomaticReconnect(true);
                mqttClient.connect(options);
                initialized = true;
                System.out.println("[FLINK] Alert sink connected to MQTT");
            }
        }

        @Override
        public void invoke(FraudAlert alert, Context context) throws Exception {
            ensureInitialized();

            String json = alert.toJson();
            MqttMessage message = new MqttMessage(json.getBytes());
            message.setQos(1);
            mqttClient.publish(OUTPUT_TOPIC, message);
            System.out.println("[FLINK] Published fraud alert: " + alert.userId);
        }
    }

    // Time semantics mode: "processing" or "event"
    private static String timeMode = "processing";

    public static void main(String[] args) throws Exception {
        // Check for command line argument to select time mode
        if (args.length > 0) {
            timeMode = args[0].toLowerCase();
        }

        System.out.println("========================================");
        System.out.println("Flink Scenario 3 - MQTT Mode");
        System.out.println("Pattern: Fraud Detection");
        System.out.println("Suspicious -> Small1 -> Small2 -> Small3 -> Withdrawal");
        System.out.println("Time Mode: " + timeMode.toUpperCase());
        System.out.println("========================================");
        System.out.println("Listening on: " + INPUT_TOPIC);
        System.out.println("Publishing to: " + OUTPUT_TOPIC);
        System.out.println("========================================");

        // Setup Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create MQTT source with appropriate time semantics
        DataStream<Transaction> txStream;

        if ("event".equals(timeMode)) {
            // EVENT TIME: Use event timestamps with watermarks
            System.out.println("[FLINK] Using EVENT TIME semantics (watermarks required)");
            txStream = env
                .addSource(new MqttSourceFunction())
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, ts) -> event.timestamp)
                        .withIdleness(Duration.ofSeconds(2))
                );
        } else {
            // PROCESSING TIME: Use wall-clock time, no watermarks
            System.out.println("[FLINK] Using PROCESSING TIME semantics (no watermarks)");
            txStream = env.addSource(new MqttSourceFunction());
        }

        // Define the fraud pattern:
        // Suspicious -> Small1 -> Small2 -> Small3 -> LargeWithdrawal
        // Using shorter windows for benchmark testing (real would use hours)
        Pattern<Transaction, ?> fraudPattern = Pattern.<Transaction>begin("suspicious")
            .where(new SimpleCondition<Transaction>() {
                @Override
                public boolean filter(Transaction tx) {
                    return tx.isSuspicious();
                }
            })
            .followedBy("small1")
            .where(new IterativeCondition<Transaction>() {
                @Override
                public boolean filter(Transaction tx, Context<Transaction> ctx) throws Exception {
                    if (!tx.isSmallPurchase()) return false;
                    for (Transaction suspicious : ctx.getEventsForPattern("suspicious")) {
                        if (suspicious.userId.equals(tx.userId)) {
                            return true;
                        }
                    }
                    return false;
                }
            })
            .within(Time.seconds(60))  // 60s for benchmark (real: 1 hour)
            .followedBy("small2")
            .where(new IterativeCondition<Transaction>() {
                @Override
                public boolean filter(Transaction tx, Context<Transaction> ctx) throws Exception {
                    if (!tx.isSmallPurchase()) return false;
                    for (Transaction suspicious : ctx.getEventsForPattern("suspicious")) {
                        if (suspicious.userId.equals(tx.userId)) {
                            return true;
                        }
                    }
                    return false;
                }
            })
            .within(Time.seconds(30))  // 30s for benchmark (real: 30 minutes)
            .followedBy("small3")
            .where(new IterativeCondition<Transaction>() {
                @Override
                public boolean filter(Transaction tx, Context<Transaction> ctx) throws Exception {
                    if (!tx.isSmallPurchase()) return false;
                    for (Transaction suspicious : ctx.getEventsForPattern("suspicious")) {
                        if (suspicious.userId.equals(tx.userId)) {
                            return true;
                        }
                    }
                    return false;
                }
            })
            .within(Time.seconds(30))  // 30s for benchmark (real: 30 minutes)
            .followedBy("withdrawal")
            .where(new IterativeCondition<Transaction>() {
                @Override
                public boolean filter(Transaction tx, Context<Transaction> ctx) throws Exception {
                    if (!tx.isLargeWithdrawal()) return false;
                    for (Transaction suspicious : ctx.getEventsForPattern("suspicious")) {
                        if (suspicious.userId.equals(tx.userId)) {
                            return true;
                        }
                    }
                    return false;
                }
            })
            .within(Time.seconds(120));  // 120s for benchmark (real: 2 hours)

        // Apply pattern
        PatternStream<Transaction> patternStream = CEP.pattern(
            txStream.keyBy(tx -> tx.userId),
            fraudPattern
        );

        // Select matches and create alerts
        DataStream<FraudAlert> alerts = patternStream.select(
            new PatternSelectFunction<Transaction, FraudAlert>() {
                @Override
                public FraudAlert select(Map<String, List<Transaction>> match) {
                    Transaction suspicious = match.get("suspicious").get(0);
                    Transaction small1 = match.get("small1").get(0);
                    Transaction small2 = match.get("small2").get(0);
                    Transaction small3 = match.get("small3").get(0);
                    Transaction withdrawal = match.get("withdrawal").get(0);

                    FraudAlert alert = new FraudAlert();
                    alert.alertType = "FRAUD_PATTERN_DETECTED";
                    alert.severity = "critical";
                    alert.userId = suspicious.userId;
                    alert.initialRiskScore = suspicious.riskScore;
                    alert.initialAmount = suspicious.amount;
                    alert.smallTxTotal = small1.amount + small2.amount + small3.amount;
                    alert.withdrawalAmount = withdrawal.amount;
                    alert.totalDurationHours = (withdrawal.timestamp - suspicious.timestamp) / 3600000.0;
                    alert.confidence = 0.95;
                    alert.recommendation = "Block account and investigate immediately";

                    return alert;
                }
            }
        );

        // Publish alerts to MQTT
        alerts.addSink(new MqttSinkFunction());

        // Also print to console for debugging
        alerts.map(FraudAlert::toJson).print();

        env.execute("Scenario 3: Fraud Detection Pattern (MQTT)");
    }
}
