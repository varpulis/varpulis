/**
 * Financial Demo Dashboard
 * Shows real data from Varpulis CEP engine
 */

import { Activity, AlertTriangle, Brain, TrendingUp, Wifi, WifiOff } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'
import PipelineGraph, { FINANCIAL_PIPELINE } from '../components/PipelineGraph'
import { useVarpulis } from '../hooks/useVarpulis'

export default function FinancialDemo() {
    const { connected, mqttConnected, events, alerts } = useVarpulis()

    // Real counts from Varpulis
    const [eventCounts, setEventCounts] = useState<Record<string, number>>({})
    const [indicatorsBySymbol, setIndicatorsBySymbol] = useState<Record<string, { sma20: number, sma50: number, rsi: number, macd: number, bbUpper: number, bbLower: number }>>({})
    const [selectedSymbol, setSelectedSymbol] = useState<string>('BTC')
    const [tradingSignals, setTradingSignals] = useState<Array<{ type: string, symbol: string, direction: string, time: string }>>([])
    const [attentionAlerts, setAttentionAlerts] = useState<Array<{ symbol: string, score: number, time: string }>>([])
    const [attentionMetrics, setAttentionMetrics] = useState<{ score: number, matches: number, total: number }>({ score: 0, matches: 0, total: 0 })
    const [recentEvents, setRecentEvents] = useState<Array<{ type: string, symbol: string, price?: number }>>([])

    const lastEventRef = useRef<string>('')
    const lastAlertRef = useRef<string>('')

    // Process events from MQTT
    useEffect(() => {
        if (events.length === 0) return
        const latestEvent = events[0]
        const eventKey = `${latestEvent.type}-${latestEvent.timestamp}`
        if (eventKey === lastEventRef.current) return
        lastEventRef.current = eventKey

        const eventType = latestEvent.type || 'Unknown'
        setEventCounts(prev => ({ ...prev, [eventType]: (prev[eventType] || 0) + 1 }))

        // Recent events feed
        setRecentEvents(prev => [{
            type: eventType,
            symbol: String(latestEvent.data?.symbol || ''),
            price: latestEvent.data?.price ? Number(latestEvent.data.price) : undefined
        }, ...prev].slice(0, 6))
    }, [events])

    // Process alerts from Varpulis
    useEffect(() => {
        if (alerts.length === 0) return
        const latestAlert = alerts[0]
        const alertKey = `${latestAlert.type}-${latestAlert.timestamp}`
        if (alertKey === lastAlertRef.current) return
        lastAlertRef.current = alertKey

        const data = (latestAlert.data?.data as Record<string, unknown>) || latestAlert.data || {}
        const alertType = String(data.signal_type || data.event_type || '')
        const rawSymbol = String(data.symbol || '')
        const symbol = rawSymbol.replace('/USD', '')

        // Update indicators by symbol
        if (symbol && (data.sma_20 || data.rsi || data.macd_line || data.upper)) {
            setIndicatorsBySymbol(prev => {
                const current = prev[symbol] || { sma20: 0, sma50: 0, rsi: 50, macd: 0, bbUpper: 0, bbLower: 0 }
                return {
                    ...prev,
                    [symbol]: {
                        sma20: data.sma_20 ? Number(data.sma_20) : current.sma20,
                        sma50: data.sma_50 ? Number(data.sma_50) : current.sma50,
                        rsi: data.rsi ? Number(data.rsi) : current.rsi,
                        macd: data.macd_line ? Number(data.macd_line) : current.macd,
                        bbUpper: data.upper ? Number(data.upper) : current.bbUpper,
                        bbLower: data.lower ? Number(data.lower) : current.bbLower,
                    }
                }
            })
        }

        // Separate PUMP_DETECTED (attention-based) from other trading signals
        if (alertType === 'PUMP_DETECTED') {
            const attentionScore = Number(data.attention_score || 0)
            setAttentionAlerts(prev => [{
                symbol: symbol || '?',
                score: attentionScore,
                time: new Date().toLocaleTimeString()
            }, ...prev].slice(0, 5))
            setAttentionMetrics(prev => ({
                score: attentionScore || prev.score,
                matches: Number(data.attention_matches || prev.matches),
                total: prev.total + 1
            }))
        } else {
            // Other trading signals
            const signalTypes = ['GOLDEN_CROSS', 'DEATH_CROSS', 'RSI_OVERBOUGHT', 'RSI_OVERSOLD', 'MACD_BULLISH', 'MACD_BEARISH', 'BB_BREAKOUT_UP', 'BB_BREAKOUT_DOWN', 'STRONG_BUY', 'STRONG_SELL']
            if (signalTypes.includes(alertType)) {
                const direction = String(data.direction || (alertType.includes('BULLISH') || alertType.includes('OVERSOLD') || alertType.includes('GOLDEN') || alertType.includes('BUY') ? 'BUY' : 'SELL'))
                setTradingSignals(prev => [{
                    type: alertType,
                    symbol: symbol || '?',
                    direction,
                    time: new Date().toLocaleTimeString()
                }, ...prev].slice(0, 8))
            }
        }
    }, [alerts])

    const totalEvents = Object.values(eventCounts).reduce((a, b) => a + b, 0)
    const ind = indicatorsBySymbol[selectedSymbol] || { sma20: 0, sma50: 0, rsi: 50, macd: 0, bbUpper: 0, bbLower: 0 }

    return (
        <div className="p-3 space-y-3 max-w-[1600px] mx-auto">
            {/* Header */}
            <div className="flex items-center justify-between bg-slate-800/50 rounded-lg px-3 py-2 border border-slate-700">
                <div className="flex items-center gap-3">
                    <h1 className="text-lg font-bold text-white">📈 Financial Markets Demo</h1>
                    <div className={`flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs ${connected ? 'bg-emerald-500/20 text-emerald-400' : 'bg-red-500/20 text-red-400'}`}>
                        {connected ? <Wifi className="w-3 h-3" /> : <WifiOff className="w-3 h-3" />}
                        {connected ? (mqttConnected ? 'MQTT' : 'WS') : 'Off'}
                    </div>
                </div>
                <div className="flex items-center gap-4 text-xs">
                    <span className="text-slate-400">Events: <span className="text-white font-mono">{totalEvents}</span></span>
                    <span className="text-slate-400">Alerts: <span className="text-yellow-400 font-mono">{alerts.length}</span></span>
                </div>
            </div>

            {/* Pipeline Graph */}
            <div className="bg-slate-800/50 rounded-lg p-2 border border-slate-700">
                <div className="text-xs text-slate-400 mb-1 px-1">
                    <strong>Pipeline:</strong> MarketTick/OHLCV → sliding windows → SMA/RSI/MACD/Bollinger → join → trading signals | Attention window detects pump patterns
                </div>
                <PipelineGraph
                    nodes={FINANCIAL_PIPELINE.nodes}
                    edges={FINANCIAL_PIPELINE.edges}
                    eventCounts={eventCounts}
                    streamCounts={{ Indicators: alerts.length, Attention: attentionMetrics.total }}
                    patternCounts={{ Signals: tradingSignals.length }}
                />
            </div>

            {/* Main Grid - 2 rows */}
            <div className="grid grid-cols-4 gap-3">
                {/* Row 1, Col 1: Events + Feed */}
                <div className="bg-slate-800/50 rounded-lg p-3 border border-slate-700">
                    <h2 className="text-sm font-semibold text-white mb-2 flex items-center gap-1.5">
                        <TrendingUp className="w-4 h-4 text-blue-400" />
                        Raw Events
                    </h2>
                    <div className="grid grid-cols-2 gap-1.5 mb-2">
                        {['MarketTick', 'OHLCV'].map(type => (
                            <div key={type} className="flex justify-between items-center bg-slate-900/50 rounded px-2 py-1">
                                <span className="text-xs text-slate-300">{type}</span>
                                <span className="text-sm font-mono text-white">{eventCounts[type] || 0}</span>
                            </div>
                        ))}
                    </div>
                    <div className="text-[10px] text-slate-500 mb-1">Live feed:</div>
                    <div className="space-y-0.5 max-h-[60px] overflow-y-auto">
                        {recentEvents.map((evt, i) => (
                            <div key={i} className="text-[10px] text-slate-400 font-mono truncate">
                                {evt.type} {evt.symbol} {evt.price ? `$${evt.price.toFixed(2)}` : ''}
                            </div>
                        ))}
                    </div>
                </div>

                {/* Row 1, Col 2: Indicators */}
                <div className="bg-slate-800/50 rounded-lg p-3 border border-slate-700">
                    <div className="flex items-center justify-between mb-2">
                        <h2 className="text-sm font-semibold text-white flex items-center gap-1.5">
                            <Activity className="w-4 h-4 text-purple-400" />
                            Indicators
                        </h2>
                        <div className="flex gap-1">
                            {['BTC', 'ETH', 'SOL'].map(sym => (
                                <button
                                    key={sym}
                                    onClick={() => setSelectedSymbol(sym)}
                                    className={`px-1.5 py-0.5 text-[10px] rounded ${selectedSymbol === sym
                                        ? 'bg-blue-500 text-white' : 'bg-slate-700 text-slate-400'}`}
                                >
                                    {sym}
                                </button>
                            ))}
                        </div>
                    </div>
                    <div className="grid grid-cols-3 gap-1.5">
                        <div className="bg-slate-900/50 rounded p-1.5">
                            <div className="text-[10px] text-slate-400">SMA20</div>
                            <div className="text-xs font-mono text-purple-400">{ind.sma20.toFixed(2)}</div>
                        </div>
                        <div className="bg-slate-900/50 rounded p-1.5">
                            <div className="text-[10px] text-slate-400">SMA50</div>
                            <div className="text-xs font-mono text-purple-400">{ind.sma50.toFixed(2)}</div>
                        </div>
                        <div className="bg-slate-900/50 rounded p-1.5">
                            <div className="text-[10px] text-slate-400">RSI</div>
                            <div className={`text-xs font-mono ${ind.rsi > 70 ? 'text-red-400' : ind.rsi < 30 ? 'text-green-400' : 'text-yellow-400'}`}>
                                {ind.rsi.toFixed(1)}
                            </div>
                        </div>
                        <div className="bg-slate-900/50 rounded p-1.5">
                            <div className="text-[10px] text-slate-400">MACD</div>
                            <div className={`text-xs font-mono ${ind.macd >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                {ind.macd.toFixed(3)}
                            </div>
                        </div>
                        <div className="bg-slate-900/50 rounded p-1.5">
                            <div className="text-[10px] text-slate-400">BB Up</div>
                            <div className="text-xs font-mono text-cyan-400">{ind.bbUpper.toFixed(2)}</div>
                        </div>
                        <div className="bg-slate-900/50 rounded p-1.5">
                            <div className="text-[10px] text-slate-400">BB Low</div>
                            <div className="text-xs font-mono text-cyan-400">{ind.bbLower.toFixed(2)}</div>
                        </div>
                    </div>
                </div>

                {/* Row 1, Col 3: Attention Engine */}
                <div className="bg-slate-800/50 rounded-lg p-3 border border-purple-500/30">
                    <h2 className="text-sm font-semibold text-white mb-1 flex items-center gap-1.5">
                        <Brain className="w-4 h-4 text-purple-400" />
                        Attention Engine
                    </h2>
                    <div className="text-[10px] text-slate-400 mb-2">
                        Multi-head attention correlates events in a 5-minute sliding window. Detects coordinated price/volume movements (pump patterns).
                    </div>
                    <div className="grid grid-cols-3 gap-1.5 mb-2">
                        <div className="bg-slate-900/50 rounded p-1.5 border border-purple-500/20">
                            <div className="text-[10px] text-slate-400">Score</div>
                            <div className="text-sm font-mono text-purple-400">{attentionMetrics.score.toFixed(2)}</div>
                        </div>
                        <div className="bg-slate-900/50 rounded p-1.5 border border-purple-500/20">
                            <div className="text-[10px] text-slate-400">Matches</div>
                            <div className="text-sm font-mono text-cyan-400">{attentionMetrics.matches}</div>
                        </div>
                        <div className="bg-slate-900/50 rounded p-1.5 border border-purple-500/20">
                            <div className="text-[10px] text-slate-400">Pumps</div>
                            <div className="text-sm font-mono text-pink-400">{attentionMetrics.total}</div>
                        </div>
                    </div>
                    <div className="text-[10px] text-slate-500">Recent detections:</div>
                    <div className="space-y-0.5 max-h-[40px] overflow-y-auto">
                        {attentionAlerts.length === 0 ? (
                            <div className="text-[10px] text-slate-600 italic">None yet</div>
                        ) : attentionAlerts.slice(0, 3).map((a, i) => (
                            <div key={i} className="text-[10px] text-purple-300 font-mono">
                                {a.symbol} score={a.score.toFixed(2)} {a.time}
                            </div>
                        ))}
                    </div>
                </div>

                {/* Row 1, Col 4: Trading Signals */}
                <div className="bg-slate-800/50 rounded-lg p-3 border border-slate-700">
                    <h2 className="text-sm font-semibold text-white mb-2 flex items-center gap-1.5">
                        <AlertTriangle className="w-4 h-4 text-yellow-400" />
                        Trading Signals ({tradingSignals.length})
                    </h2>
                    <div className="text-[10px] text-slate-500 mb-1">Cross/RSI/MACD/Bollinger signals:</div>
                    <div className="space-y-1 max-h-[130px] overflow-y-auto">
                        {tradingSignals.length === 0 ? (
                            <div className="text-xs text-slate-500 italic">Waiting for signals...</div>
                        ) : tradingSignals.map((sig, i) => (
                            <div key={i} className={`text-xs rounded px-2 py-1 flex justify-between ${sig.direction === 'BUY' ? 'bg-green-900/30 text-green-400' : 'bg-red-900/30 text-red-400'}`}>
                                <span className="font-mono">{sig.symbol}</span>
                                <span className="text-[10px]">{sig.type.replace(/_/g, ' ')}</span>
                            </div>
                        ))}
                    </div>
                </div>
            </div>

            {/* Footer */}
            <div className="text-[10px] text-slate-500 text-center">
                All data from Varpulis CEP engine via MQTT. Run: <code className="bg-slate-700 px-1 rounded">./start_demo.sh -d financial</code>
            </div>
        </div>
    )
}
