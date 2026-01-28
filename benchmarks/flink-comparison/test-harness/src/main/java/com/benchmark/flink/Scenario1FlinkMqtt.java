package com.benchmark.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.time.Duration;
import java.util.*;

/**
 * Flink implementation of Scenario 1 with MQTT input/output.
 * Aggregation: Count page views by category in sliding window (5 min, slide 30s)
 */
public class Scenario1FlinkMqtt {

    // MQTT Configuration
    private static final String MQTT_BROKER = "tcp://localhost:1883";
    private static final String INPUT_TOPIC = "benchmark/input/PageView";
    private static final String OUTPUT_TOPIC = "benchmark/output/flink";

    // PageView event
    public static class PageView implements java.io.Serializable {
        public String userId;
        public String page;
        public String category;
        public int durationMs;
        public long timestamp;

        @Override
        public String toString() {
            return String.format("PageView{user=%s, category=%s, page=%s, ts=%d}",
                    userId, category, page, timestamp);
        }
    }

    // Aggregation output
    public static class CategoryMetrics implements java.io.Serializable {
        public String metricType;
        public String category;
        public long viewCount;
        public long uniqueUsers;
        public double avgDurationMs;
        public long totalDurationMs;
        public long windowEnd;

        public String toJson() {
            return String.format(
                "{\"metric_type\":\"%s\",\"category\":\"%s\",\"view_count\":%d,\"unique_users\":%d,\"avg_duration_ms\":%.2f,\"total_duration_ms\":%d,\"window_end\":%d}",
                metricType, category, viewCount, uniqueUsers, avgDurationMs, totalDurationMs, windowEnd
            );
        }

        @Override
        public String toString() {
            return String.format("CategoryMetrics{category=%s, views=%d, unique=%d, avg=%.2f}",
                    category, viewCount, uniqueUsers, avgDurationMs);
        }
    }

    // Accumulator for aggregation
    public static class MetricsAccumulator implements java.io.Serializable {
        public String category;
        public long count = 0;
        public Set<String> uniqueUsers = new HashSet<>();
        public long totalDuration = 0;
    }

    /**
     * MQTT Source Function - receives PageView events from MQTT
     */
    public static class MqttSourceFunction implements SourceFunction<PageView> {
        private volatile boolean isRunning = true;
        private MqttClient mqttClient;

        @Override
        public void run(SourceContext<PageView> ctx) throws Exception {
            mqttClient = new MqttClient(MQTT_BROKER, "flink-scenario1-source-" + UUID.randomUUID(), new MemoryPersistence());

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
                        PageView event = parsePageView(node);
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

        private PageView parsePageView(JsonNode node) {
            PageView event = new PageView();
            event.userId = node.has("user_id") ? node.get("user_id").asText() : "";
            event.page = node.has("page") ? node.get("page").asText() : "";
            event.category = node.has("category") ? node.get("category").asText() : "unknown";
            event.durationMs = node.has("duration_ms") ? node.get("duration_ms").asInt() : 0;
            event.timestamp = node.has("ts") ? node.get("ts").asLong() : System.currentTimeMillis();
            return event;
        }
    }

    /**
     * MQTT Sink Function - publishes metrics to MQTT
     */
    public static class MqttSinkFunction implements SinkFunction<CategoryMetrics> {
        private transient MqttClient mqttClient;
        private transient boolean initialized = false;

        private void ensureInitialized() throws MqttException {
            if (!initialized) {
                mqttClient = new MqttClient(MQTT_BROKER, "flink-scenario1-sink-" + UUID.randomUUID(), new MemoryPersistence());
                MqttConnectOptions options = new MqttConnectOptions();
                options.setCleanSession(true);
                options.setAutomaticReconnect(true);
                mqttClient.connect(options);
                initialized = true;
                System.out.println("[FLINK] Metrics sink connected to MQTT");
            }
        }

        @Override
        public void invoke(CategoryMetrics metrics, Context context) throws Exception {
            ensureInitialized();

            String json = metrics.toJson();
            MqttMessage message = new MqttMessage(json.getBytes());
            message.setQos(1);
            mqttClient.publish(OUTPUT_TOPIC, message);
            System.out.println("[FLINK] Published metrics: " + metrics.category);
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
        System.out.println("Flink Scenario 1 - MQTT Mode");
        System.out.println("Aggregation: PageViews by Category");
        System.out.println("Window: 5 minutes, Slide: 30 seconds");
        System.out.println("Time Mode: " + timeMode.toUpperCase());
        System.out.println("========================================");
        System.out.println("Listening on: " + INPUT_TOPIC);
        System.out.println("Publishing to: " + OUTPUT_TOPIC);
        System.out.println("========================================");

        // Setup Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create MQTT source
        DataStream<PageView> pageViews;

        if ("event".equals(timeMode)) {
            // EVENT TIME: Use event timestamps with watermarks
            System.out.println("[FLINK] Using EVENT TIME semantics (watermarks required)");
            pageViews = env
                .addSource(new MqttSourceFunction())
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<PageView>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, ts) -> event.timestamp)
                        .withIdleness(Duration.ofSeconds(2))
                );

            // Aggregate by category with sliding window (EVENT TIME)
            DataStream<CategoryMetrics> metrics = pageViews
                .keyBy(pv -> pv.category)
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(30)))
                .aggregate(new PageViewAggregator());

            // Publish metrics to MQTT
            metrics.addSink(new MqttSinkFunction());
            metrics.map(CategoryMetrics::toJson).print();

        } else {
            // PROCESSING TIME: Use wall-clock time
            System.out.println("[FLINK] Using PROCESSING TIME semantics (no watermarks)");
            pageViews = env.addSource(new MqttSourceFunction());

            // Aggregate by category with sliding window (PROCESSING TIME)
            DataStream<CategoryMetrics> metrics = pageViews
                .keyBy(pv -> pv.category)
                .window(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.seconds(30)))
                .aggregate(new PageViewAggregator());

            // Publish metrics to MQTT
            metrics.addSink(new MqttSinkFunction());
            metrics.map(CategoryMetrics::toJson).print();
        }

        env.execute("Scenario 1: PageViews Aggregation by Category (MQTT)");
    }

    /**
     * Aggregate function for PageView events
     */
    public static class PageViewAggregator implements AggregateFunction<PageView, MetricsAccumulator, CategoryMetrics> {
        @Override
        public MetricsAccumulator createAccumulator() {
            return new MetricsAccumulator();
        }

        @Override
        public MetricsAccumulator add(PageView value, MetricsAccumulator acc) {
            acc.category = value.category;
            acc.count++;
            acc.uniqueUsers.add(value.userId);
            acc.totalDuration += value.durationMs;
            return acc;
        }

        @Override
        public CategoryMetrics getResult(MetricsAccumulator acc) {
            CategoryMetrics result = new CategoryMetrics();
            result.metricType = "PAGE_VIEWS_BY_CATEGORY";
            result.category = acc.category;
            result.viewCount = acc.count;
            result.uniqueUsers = acc.uniqueUsers.size();
            result.avgDurationMs = acc.count > 0 ? (double) acc.totalDuration / acc.count : 0;
            result.totalDurationMs = acc.totalDuration;
            result.windowEnd = System.currentTimeMillis();
            return result;
        }

        @Override
        public MetricsAccumulator merge(MetricsAccumulator a, MetricsAccumulator b) {
            a.count += b.count;
            a.uniqueUsers.addAll(b.uniqueUsers);
            a.totalDuration += b.totalDuration;
            return a;
        }
    }
}
