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
import org.apache.flink.streaming.api.windowing.time.Time;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.time.Duration;
import java.util.*;

/**
 * Flink implementation of Scenario 2: Login -> FailedTransaction
 * Reads events from JSON file and outputs matching alerts.
 */
public class Scenario2Flink {

    // Union event type
    public static class UserEvent {
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
    public static class Alert {
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
            return String.format("ALERT: user=%s, amount=%.2f, severity=%s, merchant=%s, time_delta=%dms",
                    userId, failedAmount, severity, merchant, timeBetween);
        }

        public String toJson() {
            return String.format(
                "{\"alert_type\":\"%s\",\"user_id\":\"%s\",\"login_ip\":\"%s\",\"login_device\":\"%s\",\"failed_amount\":%.2f,\"merchant\":\"%s\",\"time_between\":%d,\"severity\":\"%s\"}",
                alertType, userId, loginIp, loginDevice, failedAmount, merchant, timeBetween, severity
            );
        }
    }

    public static void main(String[] args) throws Exception {
        String inputFile = args.length > 0 ? args[0] : "data/scenario2_events.json";

        // Read events from JSON
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(new File(inputFile));
        List<UserEvent> events = new ArrayList<>();

        for (JsonNode node : root) {
            UserEvent event = new UserEvent();
            event.type = node.get("type").asText();
            event.userId = node.get("user_id").asText();
            event.timestamp = node.get("ts").asLong();

            if ("Login".equals(event.type)) {
                event.ipAddress = node.get("ip_address").asText();
                event.device = node.get("device").asText();
            } else if ("Transaction".equals(event.type)) {
                event.amount = node.get("amount").asDouble();
                event.status = node.get("status").asText();
                event.merchant = node.get("merchant").asText();
            }
            events.add(event);
        }

        // Sort by timestamp
        events.sort(Comparator.comparingLong(e -> e.timestamp));

        // Setup Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create stream from collection
        DataStream<UserEvent> eventStream = env.fromCollection(events)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserEvent>forBoundedOutOfOrderness(Duration.ofMillis(100))
                    .withTimestampAssigner((event, ts) -> event.timestamp)
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
            .within(Time.minutes(10));

        // Apply pattern
        PatternStream<UserEvent> patternStream = CEP.pattern(
            eventStream.keyBy(e -> e.userId),
            pattern
        );

        // Select matches
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

        // Print results as JSON
        alerts.map(Alert::toJson).print();

        env.execute("Scenario 2: Login -> FailedTransaction");
    }
}
