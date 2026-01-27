/**
 * Financial Demo Dashboard
 * 
 * Educational dashboard showing how Varpulis processes financial market events.
 * Displays: Raw Events → Streams → Technical Indicators → Trading Signals
 */

import { AlertTriangle, Database, Filter, TrendingUp, Wifi, WifiOff } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'
import PipelineGraph, { FINANCIAL_PIPELINE } from '../components/PipelineGraph'
import { useVarpulis, type VarpulisEvent } from '../hooks/useVarpulis'

const EVENT_TYPES = [
    { name: 'MarketTick', icon: '📈', color: 'text-blue-400' },
    { name: 'OHLCV', icon: '📊', color: 'text-green-400' },
    { name: 'OrderBook', icon: '📋', color: 'text-purple-400' },
    { name: 'NewsEvent', icon: '📰', color: 'text-yellow-400' },
]

const PATTERNS = [
    { name: 'Golden Cross', condition: 'SMA20 > SMA50', severity: 'bullish' },
    { name: 'Death Cross', condition: 'SMA20 < SMA50', severity: 'bearish' },
    { name: 'Overbought', condition: 'RSI > 70', severity: 'warning' },
    { name: 'Oversold', condition: 'RSI < 30', severity: 'bullish' },
    { name: 'MACD Bullish', condition: 'MACD crosses 0', severity: 'bullish' },
    { name: 'MACD Bearish', condition: 'MACD crosses 0', severity: 'bearish' },
    { name: 'Breakout', condition: 'BB breakout', severity: 'warning' },
]

export default function FinancialDemo() {
    const { connected, mqttConnected, events, alerts } = useVarpulis()

    const [eventCounts, setEventCounts] = useState<Record<string, number>>({})
    const [streamCounts, setStreamCounts] = useState<Record<string, number>>({})
    const [patternMatches, setPatternMatches] = useState<Record<string, number>>({})
    const [recentEvents, setRecentEvents] = useState<VarpulisEvent[]>([])
    const [marketData, setMarketData] = useState<Record<string, { price: number, change: number, volume: number }>>({})
    const [indicatorsBySymbol, setIndicatorsBySymbol] = useState<Record<string, { sma20: number, sma50: number, rsi: number, macd: number, bbUpper: number, bbLower: number }>>({})
    const [selectedSymbol, setSelectedSymbol] = useState<string>('BTC')

    const lastEventRef = useRef<string>('')
    const lastAlertRef = useRef<string>('')

    // Process alerts to extract indicators and pattern matches
    useEffect(() => {
        if (alerts.length === 0) return

        const latestAlert = alerts[0]
        const alertKey = `${latestAlert.type}-${latestAlert.timestamp}`
        if (alertKey === lastAlertRef.current) return
        lastAlertRef.current = alertKey

        // Handle nested data structure
        const nestedData = (latestAlert.data?.data as Record<string, unknown>) || {}
        const alertType = String(nestedData.event_type || latestAlert.data?.event_type || '')

        // Debug logging
        console.log('Alert received:', alertType, nestedData)

        // Update indicators by symbol (field names match VPL emit statements)
        const symbol = String(nestedData.symbol || '').replace('/USD', '')
        if (!symbol) {
            console.log('No symbol found in alert')
            return
        }
        console.log('Processing alert for symbol:', symbol, 'sma_20:', nestedData.sma_20)

        setIndicatorsBySymbol(prev => {
            const current = prev[symbol] || { sma20: 0, sma50: 0, rsi: 50, macd: 0, bbUpper: 0, bbLower: 0 }
            const updated = { ...current }

            if (nestedData.sma_20) updated.sma20 = Number(nestedData.sma_20)
            if (nestedData.sma_50) updated.sma50 = Number(nestedData.sma_50)
            if (nestedData.rsi) updated.rsi = Number(nestedData.rsi)
            if (nestedData.macd_line) updated.macd = Number(nestedData.macd_line)
            if (nestedData.upper) updated.bbUpper = Number(nestedData.upper)
            if (nestedData.lower) updated.bbLower = Number(nestedData.lower)

            return { ...prev, [symbol]: updated }
        })

        // Update pattern matches based on alert type from CEP
        if (alertType === 'GoldenCross' || alertType === 'GOLDEN_CROSS') {
            setPatternMatches(prev => ({ ...prev, 'Golden Cross': (prev['Golden Cross'] || 0) + 1 }))
        }
        if (alertType === 'DeathCross' || alertType === 'DEATH_CROSS') {
            setPatternMatches(prev => ({ ...prev, 'Death Cross': (prev['Death Cross'] || 0) + 1 }))
        }
        if (alertType === 'RSI') {
            const rsi = Number(nestedData.rsi || 0)
            if (rsi > 70) {
                setPatternMatches(prev => ({ ...prev, 'Overbought': (prev['Overbought'] || 0) + 1 }))
            } else if (rsi < 30) {
                setPatternMatches(prev => ({ ...prev, 'Oversold': (prev['Oversold'] || 0) + 1 }))
            }
        }
        if (alertType === 'RSI_OVERBOUGHT') {
            setPatternMatches(prev => ({ ...prev, 'Overbought': (prev['Overbought'] || 0) + 1 }))
        }
        if (alertType === 'RSI_OVERSOLD') {
            setPatternMatches(prev => ({ ...prev, 'Oversold': (prev['Oversold'] || 0) + 1 }))
        }
        if (alertType === 'BollingerBands') {
            setPatternMatches(prev => ({ ...prev, 'Breakout': (prev['Breakout'] || 0) + 1 }))
        }
        if (alertType === 'MACD') {
            const histogram = Number(nestedData.histogram || 0)
            if (histogram > 0) {
                setPatternMatches(prev => ({ ...prev, 'MACD Bullish': (prev['MACD Bullish'] || 0) + 1 }))
            } else {
                setPatternMatches(prev => ({ ...prev, 'MACD Bearish': (prev['MACD Bearish'] || 0) + 1 }))
            }
        }
        if (alertType === 'PumpAndDump') {
            setPatternMatches(prev => ({ ...prev, 'MACD Bullish': (prev['MACD Bullish'] || 0) + 1 }))
        }
    }, [alerts])

    useEffect(() => {
        if (events.length === 0) return

        const latestEvent = events[0]
        const eventKey = `${latestEvent.type}-${latestEvent.timestamp}`
        if (eventKey === lastEventRef.current) return
        lastEventRef.current = eventKey

        // Update event counts
        const eventType = latestEvent.type || 'Unknown'
        setEventCounts(prev => ({ ...prev, [eventType]: (prev[eventType] || 0) + 1 }))

        // Update recent events
        setRecentEvents(prev => [latestEvent, ...prev].slice(0, 20))

        // Update stream counts and market data based on event type
        if (eventType === 'MarketTick') {
            setStreamCounts(prev => ({
                ...prev,
                'Prices': (prev['Prices'] || 0) + 1,
                'SMA': (prev['SMA'] || 0) + 1,
                'Bollinger': (prev['Bollinger'] || 0) + 1,
            }))

            const symbol = String(latestEvent.data?.symbol || 'BTC/USD')
            const price = Number(latestEvent.data?.price || 0)
            const change = Number(latestEvent.data?.change_pct || 0)
            const volume = Number(latestEvent.data?.volume || 0)

            setMarketData(prev => ({
                ...prev,
                [symbol]: { price, change, volume }
            }))
        } else if (eventType === 'OHLCV') {
            setStreamCounts(prev => ({
                ...prev,
                'Candles': (prev['Candles'] || 0) + 1,
                'RSI': (prev['RSI'] || 0) + 1,
                'MACD': (prev['MACD'] || 0) + 1,
            }))
        } else if (eventType === 'OrderBook') {
            setStreamCounts(prev => ({
                ...prev,
                'Liquidity': (prev['Liquidity'] || 0) + 1,
            }))
        } else if (eventType === 'NewsEvent') {
            setStreamCounts(prev => ({
                ...prev,
                'Sentiment': (prev['Sentiment'] || 0) + 1,
            }))
        } else if (eventType === 'TradingSignal') {
            setStreamCounts(prev => ({
                ...prev,
                'Signals': (prev['Signals'] || 0) + 1,
            }))

            // Process trading signals
            const signal = String(latestEvent.data?.signal || '')
            if (signal === 'golden_cross') {
                setPatternMatches(prev => ({ ...prev, 'Golden Cross': (prev['Golden Cross'] || 0) + 1 }))
            } else if (signal === 'death_cross') {
                setPatternMatches(prev => ({ ...prev, 'Death Cross': (prev['Death Cross'] || 0) + 1 }))
            } else if (signal === 'overbought') {
                setPatternMatches(prev => ({ ...prev, 'Overbought': (prev['Overbought'] || 0) + 1 }))
            } else if (signal === 'oversold') {
                setPatternMatches(prev => ({ ...prev, 'Oversold': (prev['Oversold'] || 0) + 1 }))
            } else if (signal === 'breakout') {
                setPatternMatches(prev => ({ ...prev, 'Breakout': (prev['Breakout'] || 0) + 1 }))
            } else if (signal === 'macd_bullish') {
                setPatternMatches(prev => ({ ...prev, 'MACD Bullish': (prev['MACD Bullish'] || 0) + 1 }))
            } else if (signal === 'macd_bearish') {
                setPatternMatches(prev => ({ ...prev, 'MACD Bearish': (prev['MACD Bearish'] || 0) + 1 }))
            }
        }
    }, [events])

    const totalEvents = Object.values(eventCounts).reduce((a, b) => a + b, 0)
    const totalPatterns = Object.values(patternMatches).reduce((a, b) => a + b, 0)

    return (
        <div className="p-4 space-y-4 max-w-[1600px] mx-auto">
            {/* Header */}
            <div className="flex items-center justify-between bg-slate-800/50 rounded-lg p-3 border border-slate-700">
                <div className="flex items-center gap-4">
                    <h1 className="text-xl font-bold text-white">📈 Financial Markets Demo</h1>
                    <div className={`flex items-center gap-2 px-3 py-1 rounded-full text-sm ${connected ? 'bg-emerald-500/20 text-emerald-400' : 'bg-red-500/20 text-red-400'}`}>
                        {connected ? <Wifi className="w-4 h-4" /> : <WifiOff className="w-4 h-4" />}
                        {connected ? 'Connected' : 'Disconnected'}
                    </div>
                    {connected && mqttConnected && (
                        <div className="bg-blue-500/20 text-blue-400 px-3 py-1 rounded-full text-sm">
                            MQTT Active
                        </div>
                    )}
                </div>
                <div className="flex items-center gap-6 text-sm">
                    <span className="text-slate-400">Events: <span className="text-white font-mono font-bold">{totalEvents}</span></span>
                    <span className="text-slate-400">Signals: <span className="text-yellow-400 font-mono font-bold">{totalPatterns}</span></span>
                    <span className="text-slate-400">Alerts: <span className="text-red-400 font-mono font-bold">{alerts.length}</span></span>
                </div>
            </div>

            {/* Stream Pipeline Graph */}
            <div className="bg-slate-800/50 rounded-lg p-4 border border-slate-700">
                <h2 className="text-lg font-semibold text-white mb-2 flex items-center gap-2">
                    <Filter className="w-5 h-5 text-purple-400" />
                    Trading Signal Pipeline
                </h2>
                <div className="text-xs text-slate-400 mb-2">
                    Events flow from left to right: Market Data → Streams → Technical Indicators → Trading Signals
                </div>
                <PipelineGraph
                    nodes={FINANCIAL_PIPELINE.nodes}
                    edges={FINANCIAL_PIPELINE.edges}
                    eventCounts={eventCounts}
                    streamCounts={streamCounts}
                    patternCounts={patternMatches}
                />
            </div>

            {/* Main content - 3 columns */}
            <div className="grid grid-cols-3 gap-4">
                {/* Column 1: Market Events */}
                <div className="space-y-4">
                    <div className="bg-slate-800/50 rounded-lg p-4 border border-slate-700">
                        <h2 className="text-lg font-semibold text-white mb-3 flex items-center gap-2">
                            <Database className="w-5 h-5 text-blue-400" />
                            Market Events
                        </h2>
                        <div className="grid grid-cols-2 gap-2 mb-4">
                            {EVENT_TYPES.map(evt => (
                                <div key={evt.name} className="bg-slate-900/50 rounded p-2 border border-slate-600">
                                    <div className="flex items-center gap-2">
                                        <span>{evt.icon}</span>
                                        <span className={`text-xs font-medium ${evt.color}`}>{evt.name}</span>
                                    </div>
                                    <div className="text-lg font-mono font-bold text-white">{eventCounts[evt.name] || 0}</div>
                                </div>
                            ))}
                        </div>
                    </div>

                    {/* Live Event Feed */}
                    <div className="bg-slate-800/50 rounded-lg p-4 border border-slate-700 max-h-[300px] overflow-y-auto">
                        <h3 className="text-sm font-semibold text-slate-300 mb-2">Live Market Feed</h3>
                        <div className="space-y-1 text-xs font-mono">
                            {recentEvents.slice(0, 10).map((evt, i) => (
                                <div key={i} className="text-slate-400 truncate">
                                    <span className="text-blue-400">{evt.type}</span>
                                    <span className="text-slate-600"> | </span>
                                    <span>{String(evt.data?.symbol || '')}</span>
                                    {evt.data?.price ? <span className="text-green-400"> ${Number(evt.data.price).toFixed(2)}</span> : null}
                                    {evt.data?.change_pct ? (
                                        <span className={Number(evt.data.change_pct) >= 0 ? 'text-green-400' : 'text-red-400'}>
                                            {' '}{Number(evt.data.change_pct) >= 0 ? '+' : ''}{Number(evt.data.change_pct).toFixed(2)}%
                                        </span>
                                    ) : null}
                                </div>
                            ))}
                        </div>
                    </div>
                </div>

                {/* Column 2: Market Data & Indicators */}
                <div className="space-y-4">
                    <div className="bg-slate-800/50 rounded-lg p-4 border border-slate-700">
                        <h2 className="text-lg font-semibold text-white mb-3 flex items-center gap-2">
                            <TrendingUp className="w-5 h-5 text-green-400" />
                            Market Prices
                        </h2>
                        <div className="space-y-2">
                            {Object.entries(marketData).slice(0, 5).map(([symbol, data]) => (
                                <div key={symbol} className="bg-slate-900/50 rounded p-2 border border-slate-600">
                                    <div className="flex items-center justify-between">
                                        <span className="text-sm font-medium text-blue-300">{symbol}</span>
                                        <span className="text-sm font-mono text-white">${data.price.toLocaleString()}</span>
                                    </div>
                                    <div className="flex items-center justify-between text-xs mt-1">
                                        <span className={data.change >= 0 ? 'text-green-400' : 'text-red-400'}>
                                            {data.change >= 0 ? '+' : ''}{data.change.toFixed(2)}%
                                        </span>
                                        <span className="text-slate-500">Vol: {data.volume.toLocaleString()}</span>
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>

                    {/* Technical Indicators - Per Symbol */}
                    <div className="bg-slate-800/50 rounded-lg p-4 border border-slate-700">
                        <div className="flex items-center justify-between mb-3">
                            <h3 className="text-sm font-semibold text-slate-300">Technical Indicators</h3>
                            <div className="flex gap-1">
                                {['BTC', 'ETH', 'SOL'].map(sym => (
                                    <button
                                        key={sym}
                                        onClick={() => setSelectedSymbol(sym)}
                                        className={`px-2 py-1 text-xs rounded ${selectedSymbol === sym
                                            ? 'bg-blue-500 text-white'
                                            : 'bg-slate-700 text-slate-400 hover:bg-slate-600'}`}
                                    >
                                        {sym}
                                    </button>
                                ))}
                            </div>
                        </div>
                        {(() => {
                            const ind = indicatorsBySymbol[selectedSymbol] || { sma20: 0, sma50: 0, rsi: 50, macd: 0, bbUpper: 0, bbLower: 0 }
                            return (
                                <div className="grid grid-cols-2 gap-2">
                                    <div className="bg-slate-900/50 rounded p-2">
                                        <div className="text-xs text-slate-400">SMA 20</div>
                                        <div className="text-sm font-mono text-purple-400">{ind.sma20.toFixed(2)}</div>
                                    </div>
                                    <div className="bg-slate-900/50 rounded p-2">
                                        <div className="text-xs text-slate-400">SMA 50</div>
                                        <div className="text-sm font-mono text-purple-400">{ind.sma50.toFixed(2)}</div>
                                    </div>
                                    <div className="bg-slate-900/50 rounded p-2">
                                        <div className="text-xs text-slate-400">RSI</div>
                                        <div className={`text-sm font-mono ${ind.rsi > 70 ? 'text-red-400' : ind.rsi < 30 ? 'text-green-400' : 'text-yellow-400'}`}>
                                            {ind.rsi.toFixed(1)}
                                        </div>
                                    </div>
                                    <div className="bg-slate-900/50 rounded p-2">
                                        <div className="text-xs text-slate-400">MACD</div>
                                        <div className={`text-sm font-mono ${ind.macd >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                            {ind.macd.toFixed(2)}
                                        </div>
                                    </div>
                                    <div className="bg-slate-900/50 rounded p-2">
                                        <div className="text-xs text-slate-400">BB Upper</div>
                                        <div className="text-sm font-mono text-cyan-400">${ind.bbUpper.toFixed(2)}</div>
                                    </div>
                                    <div className="bg-slate-900/50 rounded p-2">
                                        <div className="text-xs text-slate-400">BB Lower</div>
                                        <div className="text-sm font-mono text-cyan-400">${ind.bbLower.toFixed(2)}</div>
                                    </div>
                                </div>
                            )
                        })()}
                    </div>
                </div>

                {/* Column 3: Signals & Alerts */}
                <div className="space-y-4">
                    <div className="bg-slate-800/50 rounded-lg p-4 border border-slate-700">
                        <h2 className="text-lg font-semibold text-white mb-3 flex items-center gap-2">
                            <AlertTriangle className="w-5 h-5 text-orange-400" />
                            Trading Signals
                        </h2>
                        <div className="space-y-2">
                            {PATTERNS.map(pattern => (
                                <div key={pattern.name} className={`bg-slate-900/50 rounded p-2 border ${patternMatches[pattern.name] ? 'border-yellow-500/50' : 'border-slate-600'}`}>
                                    <div className="flex items-center justify-between">
                                        <span className={`text-sm font-medium ${pattern.severity === 'bullish' ? 'text-green-400' :
                                            pattern.severity === 'bearish' ? 'text-red-400' : 'text-yellow-400'
                                            }`}>
                                            {pattern.severity === 'bullish' ? '🟢' : pattern.severity === 'bearish' ? '🔴' : '🟡'} {pattern.name}
                                        </span>
                                        <span className="text-sm font-mono text-white">{patternMatches[pattern.name] || 0}</span>
                                    </div>
                                    <div className="text-xs text-slate-500 mt-1">{pattern.condition}</div>
                                </div>
                            ))}
                        </div>
                    </div>

                    {/* Alerts */}
                    <div className="bg-slate-800/50 rounded-lg p-4 border border-slate-700">
                        <h3 className="text-sm font-semibold text-red-400 mb-2">🚨 Active Alerts ({alerts.length})</h3>
                        <div className="space-y-2 max-h-[200px] overflow-y-auto">
                            {alerts.length === 0 ? (
                                <div className="text-xs text-slate-500">No alerts detected</div>
                            ) : (
                                alerts.slice(0, 10).map((alert, i) => {
                                    // Handle nested data structure: alert.data.data contains the actual values
                                    const nestedData = (alert.data?.data as Record<string, unknown>) || {};
                                    const alertType = String(nestedData.event_type || alert.data?.event_type || alert.type || 'Alert');
                                    const message = String(nestedData.message || alert.data?.message || '');
                                    const symbol = String(nestedData.symbol || '');
                                    const value = nestedData.sma_20 || nestedData.sma_50 || nestedData.rsi || '';

                                    return (
                                        <div key={i} className="bg-emerald-900/20 rounded p-2 border border-emerald-500/30">
                                            <div className="flex justify-between items-center">
                                                <span className="text-sm font-medium text-emerald-400">{alertType}</span>
                                                {symbol && <span className="text-xs text-slate-400">{symbol}</span>}
                                            </div>
                                            {value && <div className="text-lg font-mono text-white">{Number(value).toFixed(2)}</div>}
                                            <div className="text-xs text-slate-400">{message}</div>
                                        </div>
                                    );
                                })
                            )}
                        </div>
                    </div>
                </div>
            </div>
        </div>
    )
}
