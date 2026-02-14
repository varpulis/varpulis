/**
 * HVAC Demo Dashboard - Compact Version
 * Shows real data from Varpulis CEP engine only
 */

import { AlertTriangle, Thermometer, Wifi, WifiOff } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'
import PipelineGraph, { HVAC_PIPELINE } from '../components/PipelineGraph'
import { useVarpulis } from '../hooks/useVarpulis'

export default function HVACDemo() {
    const { connected, mqttConnected, events, alerts } = useVarpulis()

    // Real counts from Varpulis
    const [eventCounts, setEventCounts] = useState<Record<string, number>>({})
    const [zoneData, setZoneData] = useState<Record<string, { temp: number, humidity: number }>>({})
    const [recentAlerts, setRecentAlerts] = useState<Array<{ type: string, zone: string, severity: string, reason: string }>>([])

    const lastEventRef = useRef<string>('')
    const lastAlertRef = useRef<string>('')

    // Process events from MQTT
    useEffect(() => {
        if (events.length === 0) return
        const latestEvent = events[0]
        const eventKey = `${latestEvent.type}-${latestEvent.timestamp}`
        if (eventKey === lastEventRef.current) return
        lastEventRef.current = eventKey

        const eventType = String(latestEvent.data?.event_type || latestEvent.type || 'unknown')
        setEventCounts(prev => ({ ...prev, [eventType]: (prev[eventType] || 0) + 1 }))

        // Update zone data
        const zone = String(latestEvent.data?.zone || '')
        if (zone) {
            if (eventType === 'TemperatureReading') {
                const temp = Number(latestEvent.data?.temperature || latestEvent.data?.value || 0)
                setZoneData(prev => ({
                    ...prev,
                    [zone]: { temp, humidity: prev[zone]?.humidity || 45 }
                }))
            }
            if (eventType === 'HumidityReading') {
                const humidity = Number(latestEvent.data?.humidity || latestEvent.data?.value || 0)
                setZoneData(prev => ({
                    ...prev,
                    [zone]: { temp: prev[zone]?.temp || 21, humidity }
                }))
            }
        }
    }, [events])

    // Process alerts from Varpulis
    useEffect(() => {
        if (alerts.length === 0) return
        const latestAlert = alerts[0]
        const alertKey = `${latestAlert.type}-${latestAlert.timestamp}`
        if (alertKey === lastAlertRef.current) return
        lastAlertRef.current = alertKey

        const data = (latestAlert.data?.data as Record<string, unknown>) || latestAlert.data || {}
        const alertType = String(data.alert_type || '')
        const zone = String(data.zone || '')
        const severity = String(data.severity || 'warning')
        const reason = String(data.reason || '')

        // HVAC alerts
        const alertTypes = ['TEMPERATURE_ANOMALY', 'SERVER_ROOM_CRITICAL', 'HUMIDITY_ANOMALY', 'HVAC_POWER_SPIKE', 'COMPRESSOR_DEGRADATION', 'REFRIGERANT_LEAK_SUSPECTED', 'FAN_MOTOR_DEGRADATION']
        if (alertTypes.includes(alertType)) {
            setRecentAlerts(prev => [{
                type: alertType,
                zone: zone || '?',
                severity,
                reason: reason || alertType
            }, ...prev].slice(0, 8))
        }

    }, [alerts])

    const totalEvents = Object.values(eventCounts).reduce((a, b) => a + b, 0)

    return (
        <div className="p-3 space-y-3 max-w-[1400px] mx-auto">
            {/* Header */}
            <div className="flex items-center justify-between bg-slate-800/50 rounded-lg px-3 py-2 border border-slate-700">
                <div className="flex items-center gap-3">
                    <h1 className="text-lg font-bold text-white">HVAC Monitoring</h1>
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

            {/* Pipeline Graph - Compact */}
            <div className="bg-slate-800/50 rounded-lg p-2 border border-slate-700">
                <div className="text-xs text-slate-400 mb-1 px-1">
                    Pipeline: Sensor events partitioned by zone, HVAC status monitored for degradation detection
                </div>
                <PipelineGraph
                    nodes={HVAC_PIPELINE.nodes}
                    edges={HVAC_PIPELINE.edges}
                    eventCounts={eventCounts}
                    streamCounts={{ Zones: Object.keys(zoneData).length }}
                    patternCounts={{ Alerts: recentAlerts.length }}
                />
            </div>

            {/* Main Grid - 3 columns */}
            <div className="grid grid-cols-3 gap-3">
                {/* Col 1: Events */}
                <div className="bg-slate-800/50 rounded-lg p-3 border border-slate-700">
                    <h2 className="text-sm font-semibold text-white mb-2 flex items-center gap-1.5">
                        <Thermometer className="w-4 h-4 text-blue-400" />
                        Raw Events
                    </h2>
                    <div className="space-y-1.5">
                        {['TemperatureReading', 'HumidityReading', 'HVACStatus', 'EnergyMeter'].map(type => (
                            <div key={type} className="flex justify-between items-center bg-slate-900/50 rounded px-2 py-1">
                                <span className="text-[10px] text-slate-300">{type.replace('Reading', '')}</span>
                                <span className="text-xs font-mono text-white">{eventCounts[type] || 0}</span>
                            </div>
                        ))}
                    </div>
                </div>

                {/* Col 2: Zones */}
                <div className="bg-slate-800/50 rounded-lg p-3 border border-slate-700">
                    <h2 className="text-sm font-semibold text-white mb-2">Zone Status</h2>
                    <div className="space-y-1.5 max-h-[130px] overflow-y-auto">
                        {Object.keys(zoneData).length === 0 ? (
                            <div className="text-xs text-slate-500 italic">Waiting for zone data...</div>
                        ) : Object.entries(zoneData).map(([zone, data]) => {
                            const isHot = data.temp > 28
                            const isCold = data.temp < 16
                            const color = isHot ? 'border-red-500 bg-red-500/10' : isCold ? 'border-blue-500 bg-blue-500/10' : 'border-emerald-500 bg-emerald-500/10'
                            return (
                                <div key={zone} className={`flex justify-between items-center rounded px-2 py-1 border ${color}`}>
                                    <span className="text-[10px] text-slate-300">{zone}</span>
                                    <span className="text-xs font-mono text-white">{data.temp.toFixed(1)}C / {data.humidity.toFixed(0)}%</span>
                                </div>
                            )
                        })}
                    </div>
                </div>

                {/* Col 3: Alerts */}
                <div className="bg-slate-800/50 rounded-lg p-3 border border-slate-700">
                    <h2 className="text-sm font-semibold text-white mb-2 flex items-center gap-1.5">
                        <AlertTriangle className="w-4 h-4 text-yellow-400" />
                        Alerts ({recentAlerts.length})
                    </h2>
                    <div className="space-y-1 max-h-[120px] overflow-y-auto">
                        {recentAlerts.length === 0 ? (
                            <div className="text-xs text-slate-500 italic">No alerts yet</div>
                        ) : recentAlerts.map((alert, i) => (
                            <div key={i} className={`text-[10px] rounded px-2 py-1 ${alert.severity === 'critical' ? 'bg-red-900/30 text-red-400' : 'bg-yellow-900/30 text-yellow-400'}`}>
                                <div className="flex justify-between">
                                    <span className="font-mono">{alert.zone}</span>
                                    <span>{alert.type.replace(/_/g, ' ')}</span>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            </div>

            {/* Footer */}
            <div className="text-[10px] text-slate-500 text-center">
                All data from Varpulis CEP engine via MQTT. Run: <code className="bg-slate-700 px-1 rounded">./start_demo.sh -d hvac</code>
            </div>
        </div>
    )
}
