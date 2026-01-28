/**
 * Apache Flink - Scénario 4: Jointure de Streams
 * Corréler les prix de deux marchés pour détecter des opportunités d'arbitrage
 *
 * LIGNES DE CODE: ~150 (vs ~40 en Varpulis)
 */

package com.benchmark.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class FlinkJoin {

    // Event POJOs
    public static class MarketTick {
        public String symbol;
        public double price;
        public int volume;
        public String exchange;
        public long timestamp;

        // Getters, setters, constructors...
    }

    // Output POJO
    public static class ArbitrageAlert {
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
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source streams from two markets
        DataStream<MarketTick> marketA = env.addSource(/* KafkaSource for Market A */);
        DataStream<MarketTick> marketB = env.addSource(/* KafkaSource for Market B */);

        // Assign timestamps and watermarks
        DataStream<MarketTick> marketAWithTs = marketA
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<MarketTick>forBoundedOutOfOrderness(Duration.ofMillis(100))
                    .withTimestampAssigner((event, ts) -> event.timestamp)
            );

        DataStream<MarketTick> marketBWithTs = marketB
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<MarketTick>forBoundedOutOfOrderness(Duration.ofMillis(100))
                    .withTimestampAssigner((event, ts) -> event.timestamp)
            );

        // Join streams on symbol within 1 second window
        DataStream<ArbitrageAlert> arbitrageOpportunities = marketAWithTs
            .join(marketBWithTs)
            .where(tick -> tick.symbol)
            .equalTo(tick -> tick.symbol)
            .window(TumblingEventTimeWindows.of(Time.seconds(1)))
            .apply(new JoinFunction<MarketTick, MarketTick, ArbitrageAlert>() {
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
            })
            .filter(alert -> alert != null);

        arbitrageOpportunities.print();
        env.execute("Arbitrage Detection");
    }
}
