import type { ScenarioDefinition } from '@/types/scenario'

export const insiderTradingScenario: ScenarioDefinition = {
  id: 'insider-trading',
  title: 'Insider Trading Surveillance',
  subtitle: 'Capital Markets',
  icon: 'mdi-chart-line-variant',
  color: 'purple',
  summary:
    'SEC mandates real-time trade surveillance. Detect suspicious trading before material news and abnormal position building.',
  vplSource: `stream TradeBeforeNews = TradeOrder as trade
    -> NewsRelease where symbol == trade.symbol as news
    .within(48h)
    .emit(
        alert_type: "trade_before_news",
        trader_id: trade.trader_id,
        symbol: trade.symbol,
        trade_amount: trade.amount,
        headline: news.headline
    )

stream AbnormalPositionBuilding = TradeOrder as first
    -> all TradeOrder where trader_id == first.trader_id as orders
    .within(48h)
    .emit(
        alert_type: "abnormal_position_building",
        trader_id: first.trader_id,
        symbol: first.symbol
    )`,
  patterns: [
    {
      name: 'Trade Before News',
      description:
        'Detects a trade order followed by a material news release for the same symbol within 48 hours. Correlates trading activity with non-public information.',
      vplSnippet: `TradeOrder as trade
    -> NewsRelease where symbol == trade.symbol as news
    .within(48h)`,
    },
    {
      name: 'Abnormal Position Building',
      description:
        'Detects a trader accumulating multiple orders within 48 hours using a Kleene closure. With 8 buy orders (1 first + 7 Kleene), Varpulis finds 127 subsequences vs 1 for traditional CEP.',
      vplSnippet: `TradeOrder as first
    -> all TradeOrder where trader_id == first.trader_id as orders
    .within(48h)`,
    },
  ],
  steps: [
    {
      title: 'Normal Market Activity',
      narration:
        'Three traders execute routine orders across different symbols. This is diversified, normal market participation. Zero alerts expected.',
      eventsText: `TradeOrder { trader_id: "trader_a", symbol: "MSFT", side: "buy", amount: 200, price: 420.00 }
BATCH 10000
TradeOrder { trader_id: "trader_b", symbol: "GOOG", side: "sell", amount: 150, price: 175.00 }
BATCH 8000
TradeOrder { trader_id: "trader_c", symbol: "AAPL", side: "buy", amount: 300, price: 195.00 }
BATCH 12000
TradeOrder { trader_id: "trader_a", symbol: "AMZN", side: "sell", amount: 100, price: 185.00 }
BATCH 10000
TradeOrder { trader_id: "trader_b", symbol: "TSLA", side: "buy", amount: 50, price: 250.00 }
BATCH 8000
TradeOrder { trader_id: "trader_c", symbol: "NVDA", side: "buy", amount: 100, price: 890.00 }`,
      expectedAlerts: [],
      phase: 'normal',
    },
    {
      title: 'Suspicious Accumulation (Kleene Star)',
      narration:
        'Trader "sus_insider" rapidly accumulates 8 buy orders for ACME, interleaved with normal trades from other traders. With 8 orders (1 first + 7 Kleene), Varpulis enumerates all 127 subsequences of the accumulation pattern \u2014 a traditional CEP engine reports just 1.',
      eventsText: `TradeOrder { trader_id: "sus_insider", symbol: "ACME", side: "buy", amount: 500, price: 45.00 }
BATCH 5000
TradeOrder { trader_id: "trader_a", symbol: "MSFT", side: "sell", amount: 100, price: 421.00 }
BATCH 4000
TradeOrder { trader_id: "sus_insider", symbol: "ACME", side: "buy", amount: 600, price: 45.10 }
BATCH 3000
TradeOrder { trader_id: "sus_insider", symbol: "ACME", side: "buy", amount: 700, price: 45.25 }
BATCH 5000
TradeOrder { trader_id: "trader_b", symbol: "GOOG", side: "buy", amount: 200, price: 176.00 }
BATCH 3000
TradeOrder { trader_id: "sus_insider", symbol: "ACME", side: "buy", amount: 800, price: 45.40 }
BATCH 4000
TradeOrder { trader_id: "sus_insider", symbol: "ACME", side: "buy", amount: 900, price: 45.60 }
BATCH 3000
TradeOrder { trader_id: "trader_c", symbol: "AAPL", side: "sell", amount: 150, price: 196.00 }
BATCH 4000
TradeOrder { trader_id: "sus_insider", symbol: "ACME", side: "buy", amount: 1000, price: 45.80 }
BATCH 3000
TradeOrder { trader_id: "sus_insider", symbol: "ACME", side: "buy", amount: 1100, price: 46.00 }
BATCH 5000
TradeOrder { trader_id: "trader_a", symbol: "AMZN", side: "buy", amount: 50, price: 186.00 }
BATCH 4000
TradeOrder { trader_id: "sus_insider", symbol: "ACME", side: "buy", amount: 1200, price: 46.20 }`,
      expectedAlerts: ['abnormal_position_building'],
      phase: 'attack',
    },
    {
      title: 'News Breaks',
      narration:
        'ACME Corp announces a merger. The engine correlates this news release with sus_insider\'s earlier buy order, completing the Trade Before News pattern. Two unrelated news events for other symbols also arrive but don\'t match any prior suspicious trades.',
      eventsText: `NewsRelease { symbol: "ACME", headline: "ACME Corp announces merger with GlobalTech", impact: "material" }
BATCH 5000
NewsRelease { symbol: "MSFT", headline: "Microsoft quarterly earnings beat estimates", impact: "moderate" }
BATCH 5000
NewsRelease { symbol: "TSLA", headline: "Tesla opens new factory in Mexico", impact: "moderate" }`,
      expectedAlerts: ['trade_before_news'],
      phase: 'attack',
    },
    {
      title: 'Clean Trader Proof',
      narration:
        'A diversified trader buys 3 different symbols \u2014 no single-symbol accumulation pattern triggers because the Kleene closure requires matching trader_id, and the position building pattern requires repeated same-trader orders. With only one order per trader-symbol pair, no abnormal_position_building fires.',
      eventsText: `TradeOrder { trader_id: "clean_trader", symbol: "IBM", side: "buy", amount: 100, price: 180.00 }
BATCH 8000
TradeOrder { trader_id: "clean_trader", symbol: "ORCL", side: "buy", amount: 200, price: 125.00 }
BATCH 8000
TradeOrder { trader_id: "clean_trader", symbol: "SAP", side: "buy", amount: 150, price: 210.00 }`,
      expectedAlerts: [],
      phase: 'negative',
    },
  ],
}
