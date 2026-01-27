/**
 * HVAC Demo Dashboard
 *
 * Real-time building automation monitoring with pattern detection.
 * All data comes from Varpulis CEP engine via MQTT - no local simulation.
 */

import { Activity, AlertTriangle, ArrowRight, Database, Filter, Thermometer, Wifi, WifiOff } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'
import PipelineGraph, { HVAC_PIPELINE } from '../components/PipelineGraph'
import { useVarpulis, type VarpulisEvent } from '../hooks/useVarpulis'

const EVENT_TYPES = [
    { name: 'TemperatureReading', icon: '🌡️', color: 'text-blue-400' },
    { name: 'HumidityReading', icon: '💧', color: 'text-cyan-400' },
    { name: 'HVACStatus', icon: '❄️', color: 'text-indigo-400' },
    { name: 'EnergyMeter', icon: '⚡', color: 'text-yellow-400' },
]

const PATTERNS = [
    { name: 'Temperature Anomaly', vplTypes: ['TEMPERATURE_ANOMALY'], severity: 'critical' },
    { name: 'Server Room Alert', vplTypes: ['SERVER_ROOM_CRITICAL'], severity: 'critical' },
    { name: 'Humidity Alert', vplTypes: ['HUMIDITY_ANOMALY'], severity: 'warning' },
    { name: 'Power Spike', vplTypes: ['HVAC_POWER_SPIKE'], severity: 'warning' },
    { name: 'Compressor Degradation', vplTypes: ['COMPRESSOR_DEGRADATION'], severity: 'warning' },
    { name: 'Refrigerant Leak', vplTypes: ['REFRIGERANT_LEAK_SUSPECTED'], severity: 'critical' },
]

const STREAMS = [
    { name: 'Temperatures', source: 'TemperatureReading', ops: ['filter zone'] },
    { name: 'ZoneTemperatures', source: 'Temperatures', ops: ['group by zone', 'window 5m'] },
    { name: 'Humidity', source: 'HumidityReading', ops: [] },
    { name: 'ZoneHumidity', source: 'Humidity', ops: ['group by zone'] },
    { name: 'ComfortIndex', source: 'JOIN(ZoneTemp, ZoneHumidity)', ops: ['calculate comfort'] },
    { name: 'HVACMetrics', source: 'HVACStatus', ops: ['sliding window 15m'] },
]

export default function HVACDemo() {
    const { connected, mqttConnected, events, alerts } = useVarpulis()

    const [eventCounts, setEventCounts] = useState<Record<string, number>>({})
    const [streamCounts, setStreamCounts] = useState<Record<string, number>>({})
    const [patternMatches, setPatternMatches] = useState<Record<string, number>>({})
    const [recentEvents, setRecentEvents] = useState<VarpulisEvent[]>([])
    const [zoneData, setZoneData] = useState<Record<string, { temp: number, humidity: number }>>({})
    const [recentAlerts, setRecentAlerts] = useState<Array<{ type: string, message: string, zone?: string, severity: string }>>([])

    const lastEventRef = useRef<string>('')
    const lastAlertRef = useRef<string>('')

    // Process incoming alerts from Varpulis engine
    useEffect(() => {
        if (alerts.length === 0) return

        const latestAlert = alerts[0]
        const alertKey = `${latestAlert.type}-${latestAlert.timestamp}`
        if (alertKey === lastAlertRef.current) return
        lastAlertRef.current = alertKey

        // Extract alert data - handle nested structure
        const alertData = (latestAlert.data?.data as Record<string, unknown>) || latestAlert.data || {}
        const alertType = String(alertData.alert_type || latestAlert.data?.alert_type || latestAlert.type || '')
        const severity = String(alertData.severity || 'warning')
        const zone = String(alertData.zone || '')
        const reason = String(alertData.reason || alertData.message || '')

        // Map VPL alert_type to dashboard pattern names
        for (const pattern of PATTERNS) {
            if (pattern.vplTypes.includes(alertType)) {
                setPatternMatches(prev => ({ ...prev, [pattern.name]: (prev[pattern.name] || 0) + 1 }))
                setRecentAlerts(prev => [{
                    type: pattern.name,
                    message: reason || `${pattern.name} detected`,
                    zone,
                    severity
                }, ...prev].slice(0, 15))
                break
            }
        }
    }, [alerts])

    // Process incoming events
    useEffect(() => {
        if (events.length === 0) return

        const latestEvent = events[0]
        const eventKey = `${latestEvent.timestamp}-${latestEvent.topic}`
        if (eventKey === lastEventRef.current) return
        lastEventRef.current = eventKey

        // Update recent events list
        setRecentEvents(prev => [latestEvent, ...prev].slice(0, 20))

        // Count by event type
        const eventType = String(latestEvent.data?.event_type || latestEvent.type || 'unknown')
        setEventCounts(prev => ({ ...prev, [eventType]: (prev[eventType] || 0) + 1 }))

        // Update zone data and stream counts based on event type
        const zone = String(latestEvent.data?.zone || '')

        if (eventType === 'TemperatureReading' && zone) {
            const temp = Number(latestEvent.data?.temperature || latestEvent.data?.value || 0)
            setZoneData(prev => ({
                ...prev,
                [zone]: { temp, humidity: prev[zone]?.humidity || 45 }
            }))
            setStreamCounts(prev => ({
                ...prev,
                'Temperatures': (prev['Temperatures'] || 0) + 1,
                'ZoneTemperatures': (prev['ZoneTemperatures'] || 0) + 1
            }))
        }

        if (eventType === 'HumidityReading' && zone) {
            const humidity = Number(latestEvent.data?.humidity || latestEvent.data?.value || 0)
            setZoneData(prev => ({
                ...prev,
                [zone]: { temp: prev[zone]?.temp || 21, humidity }
            }))
            setStreamCounts(prev => ({
                ...prev,
                'Humidity': (prev['Humidity'] || 0) + 1,
                'ZoneHumidity': (prev['ZoneHumidity'] || 0) + 1
            }))
        }

        if (eventType === 'HVACStatus') {
            setStreamCounts(prev => ({
                ...prev,
                'HVACMetrics': (prev['HVACMetrics'] || 0) + 1
            }))
        }

        if (eventType === 'EnergyMeter') {
            setStreamCounts(prev => ({
                ...prev,
                'Energy': (prev['Energy'] || 0) + 1
            }))
        }

        // Update ComfortIndex count when we have both temp and humidity
        if ((eventType === 'TemperatureReading' || eventType === 'HumidityReading') && zone) {
            const currentZone = zoneData[zone]
            if (currentZone?.temp > 0 && currentZone?.humidity > 0) {
                setStreamCounts(prev => ({
                    ...prev,
                    'ComfortIndex': (prev['ComfortIndex'] || 0) + 1
                }))
            }
        }
    }, [events, zoneData])

    const totalEvents = Object.values(eventCounts).reduce((a, b) => a + b, 0)
    const totalPatterns = Object.values(patternMatches).reduce((a, b) => a + b, 0)

    return (
        <div className="p-4 space-y-4 max-w-[1600px] mx-auto">
            {/* Header with connection status */}
            <div className="flex items-center justify-between bg-slate-800/50 rounded-lg p-3 border border-slate-700">
                <div className="flex items-center gap-4">
                    <h1 className="text-xl font-bold text-white">HVAC Monitoring Demo</h1>
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
                    <span className="text-slate-400">Patterns: <span className="text-yellow-400 font-mono font-bold">{totalPatterns}</span></span>
                    <span className="text-slate-400">Alerts: <span className="text-red-400 font-mono font-bold">{alerts.length}</span></span>
                </div>
            </div>

            {/* Stream Pipeline Graph */}
            <div className="bg-slate-800/50 rounded-lg p-4 border border-slate-700">
                <h2 className="text-lg font-semibold text-white mb-2 flex items-center gap-2">
                    <Filter className="w-5 h-5 text-purple-400" />
                    Stream Pipeline Graph
                </h2>
                <div className="text-xs text-slate-400 mb-2">
                    Events flow from left to right: Event Sources → Streams → Derived Streams → Pattern Detection
                </div>
                <PipelineGraph
                    nodes={HVAC_PIPELINE.nodes}
                    edges={HVAC_PIPELINE.edges}
                    eventCounts={eventCounts}
                    streamCounts={streamCounts}
                    patternCounts={patternMatches}
                />
            </div>

            {/* Main content - 3 columns */}
            <div className="grid grid-cols-3 gap-4">

                {/* Column 1: Incoming Events */}
                <div className="space-y-4">
                    <div className="bg-slate-800/50 rounded-lg p-4 border border-slate-700">
                        <h2 className="text-lg font-semibold text-white mb-3 flex items-center gap-2">
                            <Database className="w-5 h-5 text-blue-400" />
                            Incoming Events
                        </h2>
                        <div className="grid grid-cols-2 gap-2 mb-4">
                            {EVENT_TYPES.map(evt => (
                                <div key={evt.name} className="bg-slate-900/50 rounded p-2 border border-slate-600">
                                    <div className="flex items-center gap-2">
                                        <span>{evt.icon}</span>
                                        <span className={`text-xs font-medium ${evt.color}`}>{evt.name}</span>
                                    </div>
                                    <div className="text-xl font-mono font-bold text-white mt-1">
                                        {eventCounts[evt.name] || 0}
                                    </div>
                                </div>
                            ))}
                        </div>

                        <h3 className="text-sm font-medium text-slate-400 mb-2">Live Event Feed</h3>
                        <div className="space-y-1 max-h-[300px] overflow-y-auto">
                            {recentEvents.slice(0, 10).map((evt, i) => (
                                <div key={i} className="text-xs bg-slate-900/50 rounded px-2 py-1 font-mono border-l-2 border-blue-500">
                                    <span className="text-slate-500">{new Date(evt.timestamp).toLocaleTimeString()}</span>
                                    <span className="text-blue-400 ml-2">{String(evt.data?.event_type || evt.type || 'event')}</span>
                                    <span className="text-slate-400 ml-2">
                                        {evt.data?.zone ? `zone=${String(evt.data.zone)}` : ''}
                                        {evt.data?.temperature ? ` temp=${Number(evt.data.temperature).toFixed(1)}C` : ''}
                                        {evt.data?.value && !evt.data?.temperature ? ` val=${Number(evt.data.value).toFixed(1)}` : ''}
                                        {evt.data?.humidity ? ` humidity=${Number(evt.data.humidity).toFixed(0)}%` : ''}
                                        {evt.data?.power_kw ? ` power=${Number(evt.data.power_kw).toFixed(1)}kW` : ''}
                                    </span>
                                </div>
                            ))}
                            {recentEvents.length === 0 && (
                                <div className="text-slate-500 text-sm italic">Waiting for events...</div>
                            )}
                        </div>
                    </div>
                </div>

                {/* Column 2: Stream Pipeline */}
                <div className="space-y-4">
                    <div className="bg-slate-800/50 rounded-lg p-4 border border-slate-700">
                        <h2 className="text-lg font-semibold text-white mb-3 flex items-center gap-2">
                            <Filter className="w-5 h-5 text-purple-400" />
                            Stream Pipeline
                        </h2>
                        <div className="text-xs text-slate-400 mb-3">
                            Events flow through streams with filters, joins, and aggregations
                        </div>
                        <div className="space-y-2">
                            {STREAMS.map(stream => (
                                <div key={stream.name} className="bg-slate-900/50 rounded p-2 border border-slate-600">
                                    <div className="flex items-center justify-between">
                                        <div className="flex items-center gap-2">
                                            <ArrowRight className="w-3 h-3 text-purple-400" />
                                            <span className="text-sm font-medium text-purple-300">{stream.name}</span>
                                        </div>
                                        <span className="text-sm font-mono text-white">{streamCounts[stream.name] || 0}</span>
                                    </div>
                                    <div className="text-xs text-slate-500 mt-1">
                                        from: {stream.source}
                                        {stream.ops.length > 0 && (
                                            <span className="text-slate-600"> → {stream.ops.join(' → ')}</span>
                                        )}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>

                    {/* Zone Status */}
                    <div className="bg-slate-800/50 rounded-lg p-4 border border-slate-700">
                        <h2 className="text-lg font-semibold text-white mb-3 flex items-center gap-2">
                            <Thermometer className="w-5 h-5 text-emerald-400" />
                            Zone Status
                        </h2>
                        <div className="grid grid-cols-2 gap-2">
                            {Object.entries(zoneData).map(([zone, data]) => {
                                const isHot = data.temp > 28
                                const isCold = data.temp < 15
                                const status = isHot ? 'critical' : isCold ? 'critical' : data.temp > 25 ? 'warning' : 'normal'
                                const statusColor = status === 'critical' ? 'border-red-500 bg-red-500/10' :
                                    status === 'warning' ? 'border-yellow-500 bg-yellow-500/10' :
                                        'border-emerald-500 bg-emerald-500/10'
                                return (
                                    <div key={zone} className={`rounded p-2 border ${statusColor}`}>
                                        <div className="text-xs text-slate-400">{zone}</div>
                                        <div className="flex items-baseline gap-2">
                                            <span className="text-lg font-bold text-white">{data.temp.toFixed(1)}C</span>
                                            <span className="text-sm text-blue-400">{data.humidity.toFixed(0)}%</span>
                                        </div>
                                    </div>
                                )
                            })}
                            {Object.keys(zoneData).length === 0 && (
                                <div className="col-span-2 text-slate-500 text-sm italic">Waiting for zone data...</div>
                            )}
                        </div>
                    </div>
                </div>

                {/* Column 3: Patterns & Alerts */}
                <div className="space-y-4">
                    <div className="bg-slate-800/50 rounded-lg p-4 border border-slate-700">
                        <h2 className="text-lg font-semibold text-white mb-3 flex items-center gap-2">
                            <Activity className="w-5 h-5 text-yellow-400" />
                            Pattern Detection (from Varpulis)
                        </h2>
                        <div className="text-xs text-slate-400 mb-3">
                            Real patterns detected by Varpulis CEP engine
                        </div>
                        <div className="space-y-2">
                            {PATTERNS.map(pattern => {
                                const count = patternMatches[pattern.name] || 0
                                const isActive = count > 0
                                const severityColor = pattern.severity === 'critical' ? 'text-red-400' : 'text-yellow-400'
                                return (
                                    <div key={pattern.name} className={`rounded p-2 border ${isActive ? 'border-yellow-500 bg-yellow-500/10' : 'border-slate-600 bg-slate-900/50'}`}>
                                        <div className="flex items-center justify-between">
                                            <span className={`text-sm font-medium ${isActive ? severityColor : 'text-slate-400'}`}>
                                                {pattern.name}
                                            </span>
                                            <span className={`text-sm font-mono font-bold ${isActive ? severityColor : 'text-slate-500'}`}>
                                                {count}
                                            </span>
                                        </div>
                                        <div className="text-xs text-slate-500 mt-1 font-mono">
                                            {pattern.vplTypes.join(' | ')}
                                        </div>
                                    </div>
                                )
                            })}
                        </div>
                    </div>

                    <div className="bg-slate-800/50 rounded-lg p-4 border border-slate-700">
                        <h2 className="text-lg font-semibold text-white mb-3 flex items-center gap-2">
                            <AlertTriangle className="w-5 h-5 text-red-400" />
                            Recent Alerts ({recentAlerts.length})
                        </h2>
                        <div className="space-y-2 max-h-[250px] overflow-y-auto">
                            {recentAlerts.map((alert, i) => {
                                const isCritical = alert.severity === 'critical'
                                const color = isCritical ? 'border-red-500/50 bg-red-500/10' : 'border-yellow-500/50 bg-yellow-500/10'
                                const textColor = isCritical ? 'text-red-400' : 'text-yellow-400'
                                return (
                                    <div key={i} className={`text-xs border rounded p-2 ${color}`}>
                                        <div className={`font-medium ${textColor}`}>
                                            {isCritical ? '🔴' : '🟡'} {alert.type}
                                        </div>
                                        {alert.zone && <div className="text-slate-400">Zone: {alert.zone}</div>}
                                        <div className="text-slate-300 mt-1 font-mono">{alert.message}</div>
                                    </div>
                                )
                            })}
                            {recentAlerts.length === 0 && (
                                <div className="text-slate-500 text-sm italic">No alerts detected yet</div>
                            )}
                        </div>
                    </div>
                </div>
            </div>

            {/* Footer - Instructions */}
            <div className="bg-slate-800/30 rounded-lg p-3 border border-slate-700/50 text-xs text-slate-500">
                <strong className="text-slate-400">How it works:</strong> Events arrive via MQTT → Flow through Varpulis streams with filters/joins → Patterns are detected by the CEP engine → Alerts are generated.
                Run <code className="bg-slate-700 px-1 rounded">./start_demo.sh -d hvac</code> to generate events automatically.
            </div>
        </div>
    )
}
