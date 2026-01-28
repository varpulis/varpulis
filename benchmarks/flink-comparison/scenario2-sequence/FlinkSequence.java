/**
 * Apache Flink - Scénario 2: Pattern Temporel (Login → FailedTransaction)
 * Équivalent du pattern Varpulis: Login -> Transaction[failed] within 10m
 *
 * LIGNES DE CODE: ~120 (vs ~30 en Varpulis)
 */

package com.benchmark.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class FlinkSequence {

    // Event POJOs
    public static class Login {
        public String userId;
        public String ipAddress;
        public String device;
        public long timestamp;

        // Getters, setters, constructors...
    }

    public static class Transaction {
        public String userId;
        public double amount;
        public String status;
        public String merchant;
        public long timestamp;

        // Getters, setters, constructors...
    }

    // Union event for CEP pattern matching
    public static class UserEvent {
        public String type; // "login" or "transaction"
        public String userId;
        public Login login;
        public Transaction transaction;
        public long timestamp;
    }

    // Alert output
    public static class SuspiciousActivityAlert {
        public String alertType;
        public String userId;
        public String loginIp;
        public String loginDevice;
        public double failedAmount;
        public String merchant;
        public long timeBetween;
        public String severity;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source streams (from Kafka, etc.)
        DataStream<Login> logins = env.addSource(/* KafkaSource for logins */);
        DataStream<Transaction> transactions = env.addSource(/* KafkaSource for transactions */);

        // Convert to union type for CEP
        DataStream<UserEvent> loginEvents = logins.map(login -> {
            UserEvent event = new UserEvent();
            event.type = "login";
            event.userId = login.userId;
            event.login = login;
            event.timestamp = login.timestamp;
            return event;
        });

        DataStream<UserEvent> txEvents = transactions
            .filter(tx -> "failed".equals(tx.status))
            .map(tx -> {
                UserEvent event = new UserEvent();
                event.type = "transaction";
                event.userId = tx.userId;
                event.transaction = tx;
                event.timestamp = tx.timestamp;
                return event;
            });

        DataStream<UserEvent> allEvents = loginEvents.union(txEvents)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, ts) -> event.timestamp)
            );

        // Define CEP pattern: Login followed by FailedTransaction within 10 minutes
        Pattern<UserEvent, ?> pattern = Pattern.<UserEvent>begin("login")
            .where(new SimpleCondition<UserEvent>() {
                @Override
                public boolean filter(UserEvent event) {
                    return "login".equals(event.type);
                }
            })
            .followedBy("failedTx")
            .where(new IterativeCondition<UserEvent>() {
                @Override
                public boolean filter(UserEvent event, Context<UserEvent> ctx) throws Exception {
                    if (!"transaction".equals(event.type)) return false;

                    // Get the login event to compare userId
                    for (UserEvent login : ctx.getEventsForPattern("login")) {
                        if (login.userId.equals(event.userId)) {
                            return true;
                        }
                    }
                    return false;
                }
            })
            .within(Time.minutes(10));

        // Apply pattern and select matches
        PatternStream<UserEvent> patternStream = CEP.pattern(
            allEvents.keyBy(e -> e.userId),
            pattern
        );

        DataStream<SuspiciousActivityAlert> alerts = patternStream.select(
            new PatternSelectFunction<UserEvent, SuspiciousActivityAlert>() {
                @Override
                public SuspiciousActivityAlert select(Map<String, List<UserEvent>> match) {
                    UserEvent loginEvent = match.get("login").get(0);
                    UserEvent txEvent = match.get("failedTx").get(0);

                    SuspiciousActivityAlert alert = new SuspiciousActivityAlert();
                    alert.alertType = "LOGIN_THEN_FAILED_TX";
                    alert.userId = loginEvent.userId;
                    alert.loginIp = loginEvent.login.ipAddress;
                    alert.loginDevice = loginEvent.login.device;
                    alert.failedAmount = txEvent.transaction.amount;
                    alert.merchant = txEvent.transaction.merchant;
                    alert.timeBetween = txEvent.timestamp - loginEvent.timestamp;
                    alert.severity = txEvent.transaction.amount > 1000 ? "high" : "medium";

                    return alert;
                }
            }
        );

        alerts.print();
        env.execute("Login-FailedTransaction Pattern Detection");
    }
}
