import { Activity, AlertTriangle, DollarSign, Pause, Play, RotateCcw, Settings, TrendingDown, TrendingUp } from 'lucide-react'
import { useEffect, useState } from 'react'
import { Bar, BarChart, CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'

interface PricePoint {
    time: string
    price: number
    volume: number
    sma20: number
    sma50: number
}

interface Signal {
    id: number
    type: 'golden_cross' | 'death_cross' | 'breakout' | 'volume_spike'
    symbol: string
    message: string
    price: number
    timestamp: Date
}

export default function FinancialDemo() {
    const [isRunning, setIsRunning] = useState(false)
    const [symbol, setSymbol] = useState('BTC/USD')
    const [currentPrice, setCurrentPrice] = useState(45000)
    const [priceHistory, setPriceHistory] = useState<PricePoint[]>([])
    const [signals, setSignals] = useState<Signal[]>([])
    const [eventCount, setEventCount] = useState(0)
    const [stats, setStats] = useState({ high: 45000, low: 45000, change: 0, volume: 0 })

    const [config, setConfig] = useState({
        smaShort: 20,
        smaLong: 50,
        volumeThreshold: 1000,
        breakoutPercent: 2
    })

    const calculateSMA = (data: number[], period: number): number => {
        if (data.length < period) return data[data.length - 1] || 0
        const slice = data.slice(-period)
        return slice.reduce((a, b) => a + b, 0) / period
    }

    useEffect(() => {
        if (!isRunning) return

        const interval = setInterval(() => {
            // Generate price movement (random walk with momentum)
            const momentum = (Math.random() - 0.48) * 200
            const newPrice = Math.max(30000, Math.min(60000, currentPrice + momentum))
            const volume = Math.floor(Math.random() * 500 + 50)

            setCurrentPrice(newPrice)

            setPriceHistory(prev => {
                const prices = [...prev.map(p => p.price), newPrice]
                const sma20 = calculateSMA(prices, config.smaShort)
                const sma50 = calculateSMA(prices, config.smaLong)

                const now = new Date()
                const timeStr = `${now.getHours()}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}`

                const newPoint: PricePoint = {
                    time: timeStr,
                    price: newPrice,
                    volume,
                    sma20,
                    sma50
                }

                // Check for crossovers
                if (prev.length > 1) {
                    const lastPoint = prev[prev.length - 1]

                    // Golden Cross: SMA20 crosses above SMA50
                    if (lastPoint.sma20 <= lastPoint.sma50 && sma20 > sma50) {
                        setSignals(s => [{
                            id: Date.now(),
                            type: 'golden_cross' as const,
                            symbol,
                            message: `Golden Cross detected! SMA${config.smaShort} crossed above SMA${config.smaLong}`,
                            price: newPrice,
                            timestamp: new Date()
                        }, ...s].slice(0, 20))
                    }

                    // Death Cross: SMA20 crosses below SMA50
                    if (lastPoint.sma20 >= lastPoint.sma50 && sma20 < sma50) {
                        setSignals(s => [{
                            id: Date.now(),
                            type: 'death_cross' as const,
                            symbol,
                            message: `Death Cross detected! SMA${config.smaShort} crossed below SMA${config.smaLong}`,
                            price: newPrice,
                            timestamp: new Date()
                        }, ...s].slice(0, 20))
                    }

                    // Volume spike
                    if (volume > config.volumeThreshold) {
                        setSignals(s => [{
                            id: Date.now(),
                            type: 'volume_spike' as const,
                            symbol,
                            message: `High volume detected: ${volume} units`,
                            price: newPrice,
                            timestamp: new Date()
                        }, ...s].slice(0, 20))
                    }
                }

                return [...prev.slice(-60), newPoint]
            })

            // Update stats
            setStats(prev => ({
                high: Math.max(prev.high, newPrice),
                low: Math.min(prev.low, newPrice),
                change: ((newPrice - 45000) / 45000 * 100),
                volume: prev.volume + volume
            }))

            setEventCount(prev => prev + 1)
        }, 500)

        return () => clearInterval(interval)
    }, [isRunning, currentPrice, symbol, config])

    const reset = () => {
        setIsRunning(false)
        setCurrentPrice(45000)
        setPriceHistory([])
        setSignals([])
        setEventCount(0)
        setStats({ high: 45000, low: 45000, change: 0, volume: 0 })
    }

    const getSignalIcon = (type: string) => {
        switch (type) {
            case 'golden_cross': return <TrendingUp className="w-4 h-4 text-emerald-400" />
            case 'death_cross': return <TrendingDown className="w-4 h-4 text-red-400" />
            case 'volume_spike': return <Activity className="w-4 h-4 text-blue-400" />
            default: return <AlertTriangle className="w-4 h-4 text-yellow-400" />
        }
    }

    const getSignalColor = (type: string) => {
        switch (type) {
            case 'golden_cross': return 'bg-emerald-500/10 border-emerald-500/30'
            case 'death_cross': return 'bg-red-500/10 border-red-500/30'
            case 'volume_spike': return 'bg-blue-500/10 border-blue-500/30'
            default: return 'bg-yellow-500/10 border-yellow-500/30'
        }
    }

    return (
        <div className="p-6 max-w-7xl mx-auto">
            {/* Controls */}
            <div className="flex items-center justify-between mb-6">
                <div className="flex items-center gap-4">
                    <button
                        onClick={() => setIsRunning(!isRunning)}
                        className={`flex items-center gap-2 px-4 py-2 rounded-lg font-medium transition-colors ${isRunning
                            ? 'bg-red-500/20 text-red-400 hover:bg-red-500/30'
                            : 'bg-emerald-500/20 text-emerald-400 hover:bg-emerald-500/30'
                            }`}
                    >
                        {isRunning ? <Pause className="w-4 h-4" /> : <Play className="w-4 h-4" />}
                        {isRunning ? 'Pause' : 'Start'} Feed
                    </button>
                    <button onClick={reset} className="flex items-center gap-2 px-4 py-2 rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600">
                        <RotateCcw className="w-4 h-4" />
                        Reset
                    </button>
                    <select
                        value={symbol}
                        onChange={e => setSymbol(e.target.value)}
                        className="input-field"
                    >
                        <option value="BTC/USD">BTC/USD</option>
                        <option value="ETH/USD">ETH/USD</option>
                        <option value="AAPL">AAPL</option>
                    </select>
                </div>
                <div className="flex items-center gap-4 text-sm text-slate-400">
                    <span>Ticks: <span className="text-white font-mono">{eventCount}</span></span>
                    <span>Signals: <span className="text-white font-mono">{signals.length}</span></span>
                </div>
            </div>

            {/* Price Header */}
            <div className="card mb-6">
                <div className="flex items-center justify-between">
                    <div>
                        <div className="flex items-center gap-3">
                            <DollarSign className="w-8 h-8 text-blue-400" />
                            <div>
                                <h2 className="text-2xl font-bold text-white">{symbol}</h2>
                                <p className="text-slate-400 text-sm">Real-time Market Data</p>
                            </div>
                        </div>
                    </div>
                    <div className="text-right">
                        <div className="text-4xl font-bold text-white">${currentPrice.toLocaleString('en-US', { minimumFractionDigits: 2 })}</div>
                        <div className={`text-lg ${stats.change >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                            {stats.change >= 0 ? '+' : ''}{stats.change.toFixed(2)}%
                        </div>
                    </div>
                </div>
                <div className="grid grid-cols-4 gap-4 mt-4 pt-4 border-t border-slate-700">
                    <div className="text-center">
                        <div className="text-lg font-semibold text-emerald-400">${stats.high.toLocaleString()}</div>
                        <div className="text-xs text-slate-400">24h High</div>
                    </div>
                    <div className="text-center">
                        <div className="text-lg font-semibold text-red-400">${stats.low.toLocaleString()}</div>
                        <div className="text-xs text-slate-400">24h Low</div>
                    </div>
                    <div className="text-center">
                        <div className="text-lg font-semibold text-white">{stats.volume.toLocaleString()}</div>
                        <div className="text-xs text-slate-400">Volume</div>
                    </div>
                    <div className="text-center">
                        <div className="text-lg font-semibold text-blue-400">{priceHistory.length > 0 ? priceHistory[priceHistory.length - 1]?.sma20.toFixed(0) : '-'}</div>
                        <div className="text-xs text-slate-400">SMA{config.smaShort}</div>
                    </div>
                </div>
            </div>

            <div className="grid lg:grid-cols-3 gap-6">
                {/* Charts */}
                <div className="lg:col-span-2 space-y-6">
                    {/* Price Chart */}
                    <div className="card">
                        <h3 className="text-lg font-semibold text-white mb-4">Price & Moving Averages</h3>
                        <div className="h-72">
                            <ResponsiveContainer width="100%" height="100%">
                                <LineChart data={priceHistory}>
                                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                                    <XAxis dataKey="time" stroke="#9ca3af" fontSize={10} />
                                    <YAxis domain={['auto', 'auto']} stroke="#9ca3af" fontSize={10} tickFormatter={v => `$${(v / 1000).toFixed(0)}k`} />
                                    <Tooltip
                                        contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #475569', borderRadius: '8px' }}
                                        labelStyle={{ color: '#fff' }}
                                        formatter={(value) => value != null ? `$${Number(value).toLocaleString()}` : ''}
                                    />
                                    <Line type="monotone" dataKey="price" stroke="#3b82f6" strokeWidth={2} dot={false} name="Price" />
                                    <Line type="monotone" dataKey="sma20" stroke="#10b981" strokeWidth={1.5} dot={false} name={`SMA${config.smaShort}`} strokeDasharray="5 5" />
                                    <Line type="monotone" dataKey="sma50" stroke="#f59e0b" strokeWidth={1.5} dot={false} name={`SMA${config.smaLong}`} strokeDasharray="5 5" />
                                </LineChart>
                            </ResponsiveContainer>
                        </div>
                        <div className="flex items-center justify-center gap-6 mt-4 text-sm">
                            <span className="flex items-center gap-2"><span className="w-3 h-3 bg-blue-500 rounded-full"></span> Price</span>
                            <span className="flex items-center gap-2"><span className="w-3 h-1 bg-emerald-500"></span> SMA{config.smaShort}</span>
                            <span className="flex items-center gap-2"><span className="w-3 h-1 bg-amber-500"></span> SMA{config.smaLong}</span>
                        </div>
                    </div>

                    {/* Volume Chart */}
                    <div className="card">
                        <h3 className="text-lg font-semibold text-white mb-4">Volume</h3>
                        <div className="h-32">
                            <ResponsiveContainer width="100%" height="100%">
                                <BarChart data={priceHistory.slice(-30)}>
                                    <XAxis dataKey="time" stroke="#9ca3af" fontSize={10} />
                                    <YAxis stroke="#9ca3af" fontSize={10} />
                                    <Tooltip contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #475569', borderRadius: '8px' }} />
                                    <Bar dataKey="volume" fill="#6366f1" />
                                </BarChart>
                            </ResponsiveContainer>
                        </div>
                    </div>
                </div>

                {/* Right Panel */}
                <div className="space-y-6">
                    {/* Signals */}
                    <div className="card">
                        <h3 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
                            <Activity className="w-5 h-5 text-blue-400" />
                            Trading Signals
                        </h3>
                        <div className="space-y-2 max-h-64 overflow-y-auto">
                            {signals.length === 0 ? (
                                <p className="text-slate-400 text-sm text-center py-4">No signals yet. Start the feed to detect patterns.</p>
                            ) : (
                                signals.map(signal => (
                                    <div key={signal.id} className={`flex items-start gap-3 p-3 rounded-lg border ${getSignalColor(signal.type)}`}>
                                        {getSignalIcon(signal.type)}
                                        <div className="flex-1 min-w-0">
                                            <div className="flex items-center justify-between">
                                                <span className="text-sm font-medium text-white capitalize">{signal.type.replace('_', ' ')}</span>
                                                <span className="text-xs text-slate-500">{signal.timestamp.toLocaleTimeString()}</span>
                                            </div>
                                            <p className="text-xs text-slate-400 truncate">{signal.message}</p>
                                            <p className="text-xs text-slate-500">@ ${signal.price.toLocaleString()}</p>
                                        </div>
                                    </div>
                                ))
                            )}
                        </div>
                    </div>

                    {/* Configuration */}
                    <div className="card">
                        <h3 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
                            <Settings className="w-5 h-5 text-slate-400" />
                            Indicator Settings
                        </h3>
                        <div className="space-y-3">
                            <div>
                                <label className="block text-sm text-slate-400 mb-1">Short SMA Period</label>
                                <input
                                    type="number"
                                    value={config.smaShort}
                                    onChange={e => setConfig(prev => ({ ...prev, smaShort: Number(e.target.value) }))}
                                    className="input-field w-full"
                                />
                            </div>
                            <div>
                                <label className="block text-sm text-slate-400 mb-1">Long SMA Period</label>
                                <input
                                    type="number"
                                    value={config.smaLong}
                                    onChange={e => setConfig(prev => ({ ...prev, smaLong: Number(e.target.value) }))}
                                    className="input-field w-full"
                                />
                            </div>
                            <div>
                                <label className="block text-sm text-slate-400 mb-1">Volume Threshold</label>
                                <input
                                    type="number"
                                    value={config.volumeThreshold}
                                    onChange={e => setConfig(prev => ({ ...prev, volumeThreshold: Number(e.target.value) }))}
                                    className="input-field w-full"
                                />
                            </div>
                        </div>
                        <div className="mt-4 p-3 bg-slate-900 rounded-lg">
                            <p className="text-xs text-slate-400 font-mono">
                                stream GoldenCross from MarketTick<br />
                                &nbsp;&nbsp;where sma({config.smaShort}) &gt; sma({config.smaLong})<br />
                                &nbsp;&nbsp;&nbsp;&nbsp;and prev(sma({config.smaShort})) &lt;= prev(sma({config.smaLong}))<br />
                                &nbsp;&nbsp;emit {'{'}symbol, signal: "buy"{'}'}
                            </p>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    )
}
