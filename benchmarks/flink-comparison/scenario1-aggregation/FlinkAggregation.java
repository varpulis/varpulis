/**
 * Apache Flink - Scénario 1: Agrégation Simple
 * Compter les événements par catégorie sur une fenêtre glissante de 5 minutes
 *
 * LIGNES DE CODE: ~100 (vs ~25 en Varpulis)
 */

package com.benchmark.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

public class FlinkAggregation {

    // Event POJO
    public static class PageView {
        public String userId;
        public String page;
        public String category;
        public int durationMs;
        public long timestamp;

        // Getters, setters, constructors...
    }

    // Output POJO
    public static class CategoryMetrics {
        public String metricType;
        public String category;
        public long viewCount;
        public long uniqueUsers;
        public double avgDurationMs;
        public long totalDurationMs;
        public long windowEnd;
    }

    // Accumulator for aggregation
    public static class MetricsAccumulator {
        public String category;
        public long count = 0;
        public Set<String> uniqueUsers = new HashSet<>();
        public long totalDuration = 0;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source stream (from Kafka, etc.)
        DataStream<PageView> pageViews = env.addSource(/* KafkaSource for page views */);

        // Assign timestamps and watermarks
        DataStream<PageView> withTimestamps = pageViews
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<PageView>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, ts) -> event.timestamp)
            );

        // Aggregate by category with sliding window
        DataStream<CategoryMetrics> metrics = withTimestamps
            .keyBy(pv -> pv.category)
            .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(30)))
            .aggregate(new AggregateFunction<PageView, MetricsAccumulator, CategoryMetrics>() {
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
            });

        metrics.print();
        env.execute("Page Views Aggregation by Category");
    }
}
