package com.benchmark.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.time.Duration;
import java.util.*;

/**
 * Flink implementation of Scenario 4 with MQTT input/output.
 * Stream Join: Correlate prices from two markets to detect arbitrage opportunities
 */
public class Scenario4FlinkMqtt {

    // MQTT Configuration
    private static final String MQTT_BROKER = "tcp://localhost:1883";
    private static final String INPUT_TOPIC_A = "benchmark/input/MarketATick";
    private static final String INPUT_TOPIC_B = "benchmark/input/MarketBTick";
    private static final String OUTPUT_TOPIC = "benchmark/output/flink";

    // Market tick event
    public static class MarketTick implements java.io.Serializable {
        public String symbol;
        public double price;
        public int volume;
        public String exchange;
        public long timestamp;

        @Override
        public String toString() {
            return String.format("MarketTick{symbol=%s, price=%.2f, exchange=%s, ts=%d}",
                    symbol, price, exchange, timestamp);
        }
    }

    // Arbitrage alert output
    public static class ArbitrageAlert implements java.io.Serializable {
        public String alertType;
        public String symbol;
        public double priceA;
        public double priceB;
        public double spreadPct;
        public String exchangeA;
        public String exchangeB;
        public String buyOn;
        public String sellOn;
        public double potentialProfit;

        public String toJson() {
            return String.format(
                "{\"alert_type\":\"%s\",\"symbol\":\"%s\",\"price_a\":%.4f,\"price_b\":%.4f,\"spread_pct\":%.4f,\"exchange_a\":\"%s\",\"exchange_b\":\"%s\",\"buy_on\":\"%s\",\"sell_on\":\"%s\",\"potential_profit\":%.2f}",
                alertType, symbol, priceA, priceB, spreadPct, exchangeA, exchangeB, buyOn, sellOn, potentialProfit
            );
        }

        @Override
        public String toString() {
            return String.format("ArbitrageAlert{symbol=%s, spread=%.2f%%, buy=%s, sell=%s}",
                    symbol, spreadPct, buyOn, sellOn);
        }
    }

    /**
     * MQTT Source Function for Market A ticks
     */
    public static class MqttSourceFunctionMarketA implements SourceFunction<MarketTick> {
        private volatile boolean isRunning = true;
        private MqttClient mqttClient;

        @Override
        public void run(SourceContext<MarketTick> ctx) throws Exception {
            mqttClient = new MqttClient(MQTT_BROKER, "flink-scenario4-source-a-" + UUID.randomUUID(), new MemoryPersistence());

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            options.setAutomaticReconnect(true);

            ObjectMapper mapper = new ObjectMapper();

            mqttClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.err.println("[FLINK] MQTT connection lost (Market A): " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    try {
                        JsonNode node = mapper.readTree(message.getPayload());
                        MarketTick tick = parseMarketTick(node, "MarketA");
                        if (tick != null) {
                            synchronized (ctx.getCheckpointLock()) {
                                ctx.collectWithTimestamp(tick, tick.timestamp);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("[FLINK] Error parsing Market A message: " + e.getMessage());
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {}
            });

            mqttClient.connect(options);
            System.out.println("[FLINK] Market A source connected to MQTT");

            mqttClient.subscribe(INPUT_TOPIC_A, 1);
            System.out.println("[FLINK] Subscribed to: " + INPUT_TOPIC_A);

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
                System.err.println("[FLINK] Error disconnecting Market A: " + e.getMessage());
            }
        }

        private MarketTick parseMarketTick(JsonNode node, String defaultExchange) {
            MarketTick tick = new MarketTick();
            tick.symbol = node.has("symbol") ? node.get("symbol").asText() : "";
            tick.price = node.has("price") ? node.get("price").asDouble() : 0.0;
            tick.volume = node.has("volume") ? node.get("volume").asInt() : 0;
            tick.exchange = node.has("exchange") ? node.get("exchange").asText() : defaultExchange;
            tick.timestamp = node.has("ts") ? node.get("ts").asLong() : System.currentTimeMillis();
            return tick;
        }
    }

    /**
     * MQTT Source Function for Market B ticks
     */
    public static class MqttSourceFunctionMarketB implements SourceFunction<MarketTick> {
        private volatile boolean isRunning = true;
        private MqttClient mqttClient;

        @Override
        public void run(SourceContext<MarketTick> ctx) throws Exception {
            mqttClient = new MqttClient(MQTT_BROKER, "flink-scenario4-source-b-" + UUID.randomUUID(), new MemoryPersistence());

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            options.setAutomaticReconnect(true);

            ObjectMapper mapper = new ObjectMapper();

            mqttClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.err.println("[FLINK] MQTT connection lost (Market B): " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    try {
                        JsonNode node = mapper.readTree(message.getPayload());
                        MarketTick tick = parseMarketTick(node, "MarketB");
                        if (tick != null) {
                            synchronized (ctx.getCheckpointLock()) {
                                ctx.collectWithTimestamp(tick, tick.timestamp);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("[FLINK] Error parsing Market B message: " + e.getMessage());
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {}
            });

            mqttClient.connect(options);
            System.out.println("[FLINK] Market B source connected to MQTT");

            mqttClient.subscribe(INPUT_TOPIC_B, 1);
            System.out.println("[FLINK] Subscribed to: " + INPUT_TOPIC_B);

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
                System.err.println("[FLINK] Error disconnecting Market B: " + e.getMessage());
            }
        }

        private MarketTick parseMarketTick(JsonNode node, String defaultExchange) {
            MarketTick tick = new MarketTick();
            tick.symbol = node.has("symbol") ? node.get("symbol").asText() : "";
            tick.price = node.has("price") ? node.get("price").asDouble() : 0.0;
            tick.volume = node.has("volume") ? node.get("volume").asInt() : 0;
            tick.exchange = node.has("exchange") ? node.get("exchange").asText() : defaultExchange;
            tick.timestamp = node.has("ts") ? node.get("ts").asLong() : System.currentTimeMillis();
            return tick;
        }
    }

    /**
     * MQTT Sink Function - publishes arbitrage alerts to MQTT
     */
    public static class MqttSinkFunction implements SinkFunction<ArbitrageAlert> {
        private transient MqttClient mqttClient;
        private transient boolean initialized = false;

        private void ensureInitialized() throws MqttException {
            if (!initialized) {
                mqttClient = new MqttClient(MQTT_BROKER, "flink-scenario4-sink-" + UUID.randomUUID(), new MemoryPersistence());
                MqttConnectOptions options = new MqttConnectOptions();
                options.setCleanSession(true);
                options.setAutomaticReconnect(true);
                mqttClient.connect(options);
                initialized = true;
                System.out.println("[FLINK] Arbitrage alert sink connected to MQTT");
            }
        }

        @Override
        public void invoke(ArbitrageAlert alert, Context context) throws Exception {
            ensureInitialized();

            String json = alert.toJson();
            MqttMessage message = new MqttMessage(json.getBytes());
            message.setQos(1);
            mqttClient.publish(OUTPUT_TOPIC, message);
            System.out.println("[FLINK] Published arbitrage alert: " + alert.symbol);
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
        System.out.println("Flink Scenario 4 - MQTT Mode");
        System.out.println("Stream Join: Arbitrage Detection");
        System.out.println("Join window: 1 second tumbling");
        System.out.println("Time Mode: " + timeMode.toUpperCase());
        System.out.println("========================================");
        System.out.println("Listening on: " + INPUT_TOPIC_A + ", " + INPUT_TOPIC_B);
        System.out.println("Publishing to: " + OUTPUT_TOPIC);
        System.out.println("========================================");

        // Setup Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<ArbitrageAlert> arbitrageOpportunities;

        if ("event".equals(timeMode)) {
            // EVENT TIME: Use event timestamps with watermarks
            System.out.println("[FLINK] Using EVENT TIME semantics (watermarks required)");

            DataStream<MarketTick> marketA = env
                .addSource(new MqttSourceFunctionMarketA())
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<MarketTick>forBoundedOutOfOrderness(Duration.ofMillis(100))
                        .withTimestampAssigner((tick, ts) -> tick.timestamp)
                        .withIdleness(Duration.ofSeconds(2))
                );

            DataStream<MarketTick> marketB = env
                .addSource(new MqttSourceFunctionMarketB())
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<MarketTick>forBoundedOutOfOrderness(Duration.ofMillis(100))
                        .withTimestampAssigner((tick, ts) -> tick.timestamp)
                        .withIdleness(Duration.ofSeconds(2))
                );

            // Join streams on symbol within 1 second window (EVENT TIME)
            arbitrageOpportunities = marketA
                .join(marketB)
                .where(tick -> tick.symbol)
                .equalTo(tick -> tick.symbol)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply(new ArbitrageJoinFunction())
                .filter(alert -> alert != null);

        } else {
            // PROCESSING TIME: Use wall-clock time
            System.out.println("[FLINK] Using PROCESSING TIME semantics (no watermarks)");

            DataStream<MarketTick> marketA = env.addSource(new MqttSourceFunctionMarketA());
            DataStream<MarketTick> marketB = env.addSource(new MqttSourceFunctionMarketB());

            // Join streams on symbol within 1 second window (PROCESSING TIME)
            arbitrageOpportunities = marketA
                .join(marketB)
                .where(tick -> tick.symbol)
                .equalTo(tick -> tick.symbol)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .apply(new ArbitrageJoinFunction())
                .filter(alert -> alert != null);
        }

        // Publish alerts to MQTT
        arbitrageOpportunities.addSink(new MqttSinkFunction());

        // Also print to console for debugging
        arbitrageOpportunities.map(ArbitrageAlert::toJson).print();

        env.execute("Scenario 4: Arbitrage Detection via Stream Join (MQTT)");
    }

    /**
     * Join function to detect arbitrage opportunities
     */
    public static class ArbitrageJoinFunction implements JoinFunction<MarketTick, MarketTick, ArbitrageAlert> {
        @Override
        public ArbitrageAlert join(MarketTick a, MarketTick b) {
            double minPrice = Math.min(a.price, b.price);
            double spreadPct = Math.abs(a.price - b.price) / minPrice * 100;

            // Only emit if spread > 1%
            if (spreadPct <= 1.0) {
                return null;
            }

            ArbitrageAlert alert = new ArbitrageAlert();
            alert.alertType = "ARBITRAGE_OPPORTUNITY";
            alert.symbol = a.symbol;
            alert.priceA = a.price;
            alert.priceB = b.price;
            alert.spreadPct = spreadPct;
            alert.exchangeA = a.exchange;
            alert.exchangeB = b.exchange;
            alert.buyOn = a.price < b.price ? a.exchange : b.exchange;
            alert.sellOn = a.price > b.price ? a.exchange : b.exchange;
            alert.potentialProfit = Math.abs(a.price - b.price) * Math.min(a.volume, b.volume);

            return alert;
        }
    }
}
