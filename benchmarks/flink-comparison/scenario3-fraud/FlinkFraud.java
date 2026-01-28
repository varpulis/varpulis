/**
 * Apache Flink - Scénario 3: Détection de Fraude Multi-Événements
 * Pattern: Transaction suspicieuse → 3+ petites transactions → Gros retrait
 *
 * LIGNES DE CODE: ~200 (vs ~40 en Varpulis)
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

public class FlinkFraud {

    // Transaction POJO
    public static class Transaction {
        public String userId;
        public double amount;
        public String type; // "purchase", "transfer", "withdrawal", "deposit"
        public String merchant;
        public String location;
        public double riskScore;
        public long timestamp;

        // Getters, setters, constructors...
        public boolean isSuspicious() {
            return riskScore > 0.7 || amount > 5000;
        }

        public boolean isSmallPurchase() {
            return amount < 100 && "purchase".equals(type);
        }

        public boolean isLargeWithdrawal() {
            return "withdrawal".equals(type) && amount > 1000;
        }
    }

    // Alert output
    public static class FraudAlert {
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
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source stream from Kafka
        DataStream<Transaction> transactions = env.addSource(/* KafkaSource */)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner((tx, ts) -> tx.timestamp)
            );

        // Key by user for pattern matching
        DataStream<Transaction> keyedTransactions = transactions.keyBy(tx -> tx.userId);

        // Define the fraud pattern
        // Pattern: Suspicious -> Small1 -> Small2 -> Small3 -> LargeWithdrawal
        // All within time constraints
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
            .within(Time.hours(1))
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
            .within(Time.minutes(30))
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
            .within(Time.minutes(30))
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
            .within(Time.hours(2));

        // Apply pattern
        PatternStream<Transaction> patternStream = CEP.pattern(keyedTransactions, fraudPattern);

        // Select and create alerts
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
                    alert.totalDurationHours =
                        (withdrawal.timestamp - suspicious.timestamp) / 3600000.0;
                    alert.confidence = 0.95;
                    alert.recommendation = "Block account and investigate immediately";

                    return alert;
                }
            }
        );

        // Output alerts
        alerts.addSink(/* KafkaSink or AlertService */);

        env.execute("Fraud Detection Pipeline");
    }
}

/*
 * COMPARAISON:
 *
 * | Aspect               | Varpulis        | Flink           |
 * |----------------------|-----------------|-----------------|
 * | Lignes de code       | ~40             | ~200            |
 * | Temps de développement| Minutes        | Heures          |
 * | Lisibilité           | Excellent       | Moyenne         |
 * | Patterns temporels   | Natif (->)      | API complexe    |
 * | Conditions inter-evt | Implicite       | IterativeCondition |
 * | Configuration        | 3 lignes        | ~20 lignes      |
 *
 * Points forts Varpulis:
 * - Syntaxe déclarative intuitive
 * - Opérateur -> pour les séquences
 * - Références implicites entre événements (suspicious.user_id)
 * - Pas besoin de classes Java/Scala
 *
 * Points forts Flink:
 * - Scalabilité horizontale
 * - Exactly-once semantics
 * - Écosystème mature
 * - State backends avancés
 */
