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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Flink implementation of Scenario 2 with MQTT input/output.
 * Pattern: Login -> FailedTransaction within 10 minutes
 */
public class Scenario2FlinkMqtt {

    // MQTT Configuration
    private static final String MQTT_BROKER = "tcp://localhost:1883";
    private static final String INPUT_TOPIC_LOGIN = "benchmark/input/Login";
    private static final String INPUT_TOPIC_TX = "benchmark/input/Transaction";
    private static final String OUTPUT_TOPIC = "benchmark/output/flink";

    // Shared event queue for MQTT source
    private static final BlockingQueue<UserEvent> eventQueue = new LinkedBlockingQueue<>();
    private static volatile boolean running = true;

    // Union event type
    public static class UserEvent implements java.io.Serializable {
        public String type;
        public String userId;
        public String ipAddress;
        public String device;
        public double amount;
        public String status;
        public String merchant;
        public long timestamp;

        @Override
        public String toString() {
            return String.format("UserEvent{type=%s, userId=%s, ts=%d}", type, userId, timestamp);
        }
    }

    // Alert output
    public static class Alert implements java.io.Serializable {
        public String alertType;
        public String userId;
        public String loginIp;
        public String loginDevice;
        public double failedAmount;
        public String merchant;
        public long timeBetween;
        public String severity;

        @Override
        public String toString() {
            return String.format("ALERT: user=%s, amount=%.2f, severity=%s, merchant=%s",
                    userId, failedAmount, severity, merchant);
        }

        public String toJson() {
            return String.format(
                "{\"alert_type\":\"%s\",\"user_id\":\"%s\",\"login_ip\":\"%s\",\"login_device\":\"%s\",\"failed_amount\":%.2f,\"merchant\":\"%s\",\"time_between\":%d,\"severity\":\"%s\"}",
                alertType, userId, loginIp, loginDevice, failedAmount, merchant, timeBetween, severity
            );
        }
    }

    /**
     * MQTT Source Function - receives events from MQTT and emits to Flink
     */
    public static class MqttSourceFunction implements SourceFunction<UserEvent> {
        private volatile boolean isRunning = true;
        private MqttClient mqttClient;

        @Override
        public void run(SourceContext<UserEvent> ctx) throws Exception {
            mqttClient = new MqttClient(MQTT_BROKER, "flink-scenario2-source-" + UUID.randomUUID(), new MemoryPersistence());

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
                        UserEvent event = parseEvent(node);
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

            mqttClient.subscribe(INPUT_TOPIC_LOGIN, 1);
            mqttClient.subscribe(INPUT_TOPIC_TX, 1);
            System.out.println("[FLINK] Subscribed to: " + INPUT_TOPIC_LOGIN + ", " + INPUT_TOPIC_TX);

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

        private UserEvent parseEvent(JsonNode node) {
            UserEvent event = new UserEvent();
            event.type = node.has("type") ? node.get("type").asText() : "Unknown";
            event.userId = node.has("user_id") ? node.get("user_id").asText() : "";
            event.timestamp = node.has("ts") ? node.get("ts").asLong() : System.currentTimeMillis();

            if ("Login".equals(event.type)) {
                event.ipAddress = node.has("ip_address") ? node.get("ip_address").asText() : "";
                event.device = node.has("device") ? node.get("device").asText() : "";
            } else if ("Transaction".equals(event.type)) {
                event.amount = node.has("amount") ? node.get("amount").asDouble() : 0.0;
                event.status = node.has("status") ? node.get("status").asText() : "";
                event.merchant = node.has("merchant") ? node.get("merchant").asText() : "";
            }

            return event;
        }
    }

    /**
     * MQTT Sink Function - publishes alerts to MQTT
     */
    public static class MqttSinkFunction implements SinkFunction<Alert> {
        private transient MqttClient mqttClient;
        private transient boolean initialized = false;

        private void ensureInitialized() throws MqttException {
            if (!initialized) {
                mqttClient = new MqttClient(MQTT_BROKER, "flink-scenario2-sink-" + UUID.randomUUID(), new MemoryPersistence());
                MqttConnectOptions options = new MqttConnectOptions();
                options.setCleanSession(true);
                options.setAutomaticReconnect(true);
                mqttClient.connect(options);
                initialized = true;
                System.out.println("[FLINK] Alert sink connected to MQTT");
            }
        }

        @Override
        public void invoke(Alert alert, Context context) throws Exception {
            ensureInitialized();

            String json = alert.toJson();
            MqttMessage message = new MqttMessage(json.getBytes());
            message.setQos(1);
            mqttClient.publish(OUTPUT_TOPIC, message);
            System.out.println("[FLINK] Published alert: " + alert.userId);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("========================================");
        System.out.println("Flink Scenario 2 - MQTT Mode");
        System.out.println("Pattern: Login -> FailedTransaction");
        System.out.println("========================================");
        System.out.println("Listening on: " + INPUT_TOPIC_LOGIN + ", " + INPUT_TOPIC_TX);
        System.out.println("Publishing to: " + OUTPUT_TOPIC);
        System.out.println("========================================");

        // Setup Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create MQTT source
        // forMonotonousTimestamps because benchmark events arrive in order
        // withIdleness allows watermark to progress even when no new events arrive
        DataStream<UserEvent> eventStream = env
            .addSource(new MqttSourceFunction())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserEvent>forMonotonousTimestamps()
                    .withTimestampAssigner((event, ts) -> event.timestamp)
                    .withIdleness(Duration.ofSeconds(2))
            );

        // Define CEP pattern: Login followed by FailedTransaction within 10 minutes
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
                        if (login.userId.equals(event.userId)) {
                            return true;
                        }
                    }
                    return false;
                }
            })
            .within(Time.seconds(5));  // Short window for benchmark (real would be 10 minutes)

        // Apply pattern
        PatternStream<UserEvent> patternStream = CEP.pattern(
            eventStream.keyBy(e -> e.userId),
            pattern
        );

        // Select matches and publish to MQTT
        DataStream<Alert> alerts = patternStream.select(
            new PatternSelectFunction<UserEvent, Alert>() {
                @Override
                public Alert select(Map<String, List<UserEvent>> match) {
                    UserEvent login = match.get("login").get(0);
                    UserEvent tx = match.get("failedTx").get(0);

                    Alert alert = new Alert();
                    alert.alertType = "LOGIN_THEN_FAILED_TX";
                    alert.userId = login.userId;
                    alert.loginIp = login.ipAddress;
                    alert.loginDevice = login.device;
                    alert.failedAmount = tx.amount;
                    alert.merchant = tx.merchant;
                    alert.timeBetween = tx.timestamp - login.timestamp;
                    alert.severity = tx.amount > 1000 ? "high" : "medium";

                    return alert;
                }
            }
        );

        // Publish alerts to MQTT
        alerts.addSink(new MqttSinkFunction());

        // Also print to console for debugging
        alerts.map(Alert::toJson).print();

        env.execute("Scenario 2: Login -> FailedTransaction (MQTT)");
    }
}
