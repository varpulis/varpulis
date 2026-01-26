import { AlertTriangle, Pause, Play, RotateCcw, Settings, Thermometer, Zap } from 'lucide-react'
import { useEffect, useState } from 'react'
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'

interface ZoneData {
    zone: string
    temperature: number
    humidity: number
    setpoint: number
    status: 'normal' | 'warning' | 'critical'
}

interface Alert {
    id: number
    type: string
    zone: string
    message: string
    severity: 'info' | 'warning' | 'critical'
    timestamp: Date
}

interface TimeSeriesPoint {
    time: string
    Zone_A: number
    Zone_B: number
    Zone_C: number
}

export default function HVACDemo() {
    const [isRunning, setIsRunning] = useState(false)
    const [zones, setZones] = useState<ZoneData[]>([
        { zone: 'Zone_A', temperature: 21.5, humidity: 45, setpoint: 22, status: 'normal' },
        { zone: 'Zone_B', temperature: 22.0, humidity: 48, setpoint: 22, status: 'normal' },
        { zone: 'Zone_C', temperature: 21.8, humidity: 46, setpoint: 22, status: 'normal' },
    ])
    const [alerts, setAlerts] = useState<Alert[]>([])
    const [temperatureHistory, setTemperatureHistory] = useState<TimeSeriesPoint[]>([])
    const [energyData, setEnergyData] = useState({ current: 3.2, daily: 45.6, peak: 5.1 })
    const [eventCount, setEventCount] = useState(0)

    // Configuration
    const [config, setConfig] = useState({
        tempThresholdHigh: 28,
        tempThresholdLow: 15,
        humidityThreshold: 70,
        energyAlertKw: 5
    })

    useEffect(() => {
        if (!isRunning) return

        const interval = setInterval(() => {
            // Generate new temperature readings
            setZones(prev => prev.map(zone => {
                const variation = (Math.random() - 0.5) * 2
                const newTemp = Math.max(10, Math.min(35, zone.temperature + variation))
                const newHumidity = Math.max(20, Math.min(80, zone.humidity + (Math.random() - 0.5) * 5))

                let status: 'normal' | 'warning' | 'critical' = 'normal'
                if (newTemp > config.tempThresholdHigh || newTemp < config.tempThresholdLow) {
                    status = 'critical'
                } else if (Math.abs(newTemp - zone.setpoint) > 3) {
                    status = 'warning'
                }

                // Generate alerts
                if (status === 'critical' && zone.status !== 'critical') {
                    const alert: Alert = {
                        id: Date.now() + Math.random(),
                        type: 'temperature_anomaly',
                        zone: zone.zone,
                        message: `Temperature ${newTemp.toFixed(1)}°C exceeds threshold`,
                        severity: 'critical',
                        timestamp: new Date()
                    }
                    setAlerts(prev => [alert, ...prev].slice(0, 20))
                }

                return { ...zone, temperature: newTemp, humidity: newHumidity, status }
            }))

            // Update temperature history
            setTemperatureHistory(prev => {
                const now = new Date()
                const timeStr = `${now.getHours()}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}`
                const newPoint: TimeSeriesPoint = {
                    time: timeStr,
                    Zone_A: zones[0].temperature,
                    Zone_B: zones[1].temperature,
                    Zone_C: zones[2].temperature,
                }
                return [...prev.slice(-30), newPoint]
            })

            // Update energy data
            setEnergyData(prev => ({
                current: Math.max(1, Math.min(8, prev.current + (Math.random() - 0.5) * 0.5)),
                daily: prev.daily + 0.01,
                peak: Math.max(prev.peak, prev.current)
            }))

            setEventCount(prev => prev + 1)
        }, 1000)

        return () => clearInterval(interval)
    }, [isRunning, zones, config])

    const reset = () => {
        setIsRunning(false)
        setZones([
            { zone: 'Zone_A', temperature: 21.5, humidity: 45, setpoint: 22, status: 'normal' },
            { zone: 'Zone_B', temperature: 22.0, humidity: 48, setpoint: 22, status: 'normal' },
            { zone: 'Zone_C', temperature: 21.8, humidity: 46, setpoint: 22, status: 'normal' },
        ])
        setAlerts([])
        setTemperatureHistory([])
        setEventCount(0)
    }

    const getStatusColor = (status: string) => {
        switch (status) {
            case 'critical': return 'text-red-400 bg-red-500/10 border-red-500/30'
            case 'warning': return 'text-yellow-400 bg-yellow-500/10 border-yellow-500/30'
            default: return 'text-emerald-400 bg-emerald-500/10 border-emerald-500/30'
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
                        {isRunning ? 'Pause' : 'Start'} Simulation
                    </button>
                    <button onClick={reset} className="flex items-center gap-2 px-4 py-2 rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600">
                        <RotateCcw className="w-4 h-4" />
                        Reset
                    </button>
                </div>
                <div className="flex items-center gap-4 text-sm text-slate-400">
                    <span>Events: <span className="text-white font-mono">{eventCount}</span></span>
                    <span>Alerts: <span className="text-white font-mono">{alerts.length}</span></span>
                </div>
            </div>

            <div className="grid lg:grid-cols-3 gap-6">
                {/* Left: Zone Cards */}
                <div className="space-y-4">
                    <h3 className="text-lg font-semibold text-white flex items-center gap-2">
                        <Thermometer className="w-5 h-5 text-emerald-400" />
                        Temperature Zones
                    </h3>
                    {zones.map(zone => (
                        <div key={zone.zone} className={`card border ${getStatusColor(zone.status)}`}>
                            <div className="flex items-center justify-between mb-3">
                                <span className="font-medium text-white">{zone.zone.replace('_', ' ')}</span>
                                <span className={`text-xs px-2 py-1 rounded ${getStatusColor(zone.status)}`}>
                                    {zone.status.toUpperCase()}
                                </span>
                            </div>
                            <div className="grid grid-cols-3 gap-4 text-center">
                                <div>
                                    <div className="text-2xl font-bold text-white">{zone.temperature.toFixed(1)}°</div>
                                    <div className="text-xs text-slate-400">Current</div>
                                </div>
                                <div>
                                    <div className="text-2xl font-bold text-slate-400">{zone.setpoint}°</div>
                                    <div className="text-xs text-slate-400">Setpoint</div>
                                </div>
                                <div>
                                    <div className="text-2xl font-bold text-blue-400">{zone.humidity.toFixed(0)}%</div>
                                    <div className="text-xs text-slate-400">Humidity</div>
                                </div>
                            </div>
                        </div>
                    ))}

                    {/* Energy Card */}
                    <div className="card">
                        <h4 className="font-medium text-white mb-3 flex items-center gap-2">
                            <Zap className="w-4 h-4 text-yellow-400" />
                            Energy Consumption
                        </h4>
                        <div className="grid grid-cols-3 gap-4 text-center">
                            <div>
                                <div className="text-xl font-bold text-yellow-400">{energyData.current.toFixed(1)}</div>
                                <div className="text-xs text-slate-400">kW Now</div>
                            </div>
                            <div>
                                <div className="text-xl font-bold text-white">{energyData.daily.toFixed(1)}</div>
                                <div className="text-xs text-slate-400">kWh Today</div>
                            </div>
                            <div>
                                <div className="text-xl font-bold text-red-400">{energyData.peak.toFixed(1)}</div>
                                <div className="text-xs text-slate-400">kW Peak</div>
                            </div>
                        </div>
                    </div>
                </div>

                {/* Center: Charts */}
                <div className="lg:col-span-2 space-y-6">
                    {/* Temperature Chart */}
                    <div className="card">
                        <h3 className="text-lg font-semibold text-white mb-4">Temperature History</h3>
                        <div className="h-64">
                            <ResponsiveContainer width="100%" height="100%">
                                <LineChart data={temperatureHistory}>
                                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                                    <XAxis dataKey="time" stroke="#9ca3af" fontSize={10} />
                                    <YAxis domain={[15, 30]} stroke="#9ca3af" fontSize={10} />
                                    <Tooltip
                                        contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #475569', borderRadius: '8px' }}
                                        labelStyle={{ color: '#fff' }}
                                    />
                                    <Line type="monotone" dataKey="Zone_A" stroke="#10b981" strokeWidth={2} dot={false} />
                                    <Line type="monotone" dataKey="Zone_B" stroke="#3b82f6" strokeWidth={2} dot={false} />
                                    <Line type="monotone" dataKey="Zone_C" stroke="#f59e0b" strokeWidth={2} dot={false} />
                                </LineChart>
                            </ResponsiveContainer>
                        </div>
                        <div className="flex items-center justify-center gap-6 mt-4 text-sm">
                            <span className="flex items-center gap-2"><span className="w-3 h-3 bg-emerald-500 rounded-full"></span> Zone A</span>
                            <span className="flex items-center gap-2"><span className="w-3 h-3 bg-blue-500 rounded-full"></span> Zone B</span>
                            <span className="flex items-center gap-2"><span className="w-3 h-3 bg-amber-500 rounded-full"></span> Zone C</span>
                        </div>
                    </div>

                    {/* Alerts */}
                    <div className="card">
                        <h3 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
                            <AlertTriangle className="w-5 h-5 text-yellow-400" />
                            Real-time Alerts
                        </h3>
                        <div className="space-y-2 max-h-48 overflow-y-auto">
                            {alerts.length === 0 ? (
                                <p className="text-slate-400 text-sm text-center py-4">No alerts yet. Start the simulation to generate events.</p>
                            ) : (
                                alerts.map(alert => (
                                    <div key={alert.id} className={`flex items-center justify-between p-3 rounded-lg ${alert.severity === 'critical' ? 'bg-red-500/10 border border-red-500/30' : 'bg-yellow-500/10 border border-yellow-500/30'
                                        }`}>
                                        <div>
                                            <span className={`text-sm font-medium ${alert.severity === 'critical' ? 'text-red-400' : 'text-yellow-400'}`}>
                                                [{alert.zone}] {alert.type}
                                            </span>
                                            <p className="text-xs text-slate-400">{alert.message}</p>
                                        </div>
                                        <span className="text-xs text-slate-500">{alert.timestamp.toLocaleTimeString()}</span>
                                    </div>
                                ))
                            )}
                        </div>
                    </div>

                    {/* Configuration */}
                    <div className="card">
                        <h3 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
                            <Settings className="w-5 h-5 text-slate-400" />
                            VPL Configuration
                        </h3>
                        <div className="grid md:grid-cols-2 gap-4">
                            <div>
                                <label className="block text-sm text-slate-400 mb-1">High Temp Threshold (°C)</label>
                                <input
                                    type="number"
                                    value={config.tempThresholdHigh}
                                    onChange={e => setConfig(prev => ({ ...prev, tempThresholdHigh: Number(e.target.value) }))}
                                    className="input-field w-full"
                                />
                            </div>
                            <div>
                                <label className="block text-sm text-slate-400 mb-1">Low Temp Threshold (°C)</label>
                                <input
                                    type="number"
                                    value={config.tempThresholdLow}
                                    onChange={e => setConfig(prev => ({ ...prev, tempThresholdLow: Number(e.target.value) }))}
                                    className="input-field w-full"
                                />
                            </div>
                            <div>
                                <label className="block text-sm text-slate-400 mb-1">Humidity Threshold (%)</label>
                                <input
                                    type="number"
                                    value={config.humidityThreshold}
                                    onChange={e => setConfig(prev => ({ ...prev, humidityThreshold: Number(e.target.value) }))}
                                    className="input-field w-full"
                                />
                            </div>
                            <div>
                                <label className="block text-sm text-slate-400 mb-1">Energy Alert (kW)</label>
                                <input
                                    type="number"
                                    value={config.energyAlertKw}
                                    onChange={e => setConfig(prev => ({ ...prev, energyAlertKw: Number(e.target.value) }))}
                                    className="input-field w-full"
                                />
                            </div>
                        </div>
                        <div className="mt-4 p-3 bg-slate-900 rounded-lg">
                            <p className="text-xs text-slate-400 font-mono">
                                stream TemperatureAnomalies from TemperatureReading<br />
                                &nbsp;&nbsp;where value &gt; {config.tempThresholdHigh} or value &lt; {config.tempThresholdLow}<br />
                                &nbsp;&nbsp;emit {'{'}zone, value, alert_type: "temperature_anomaly"{'}'}
                            </p>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    )
}
