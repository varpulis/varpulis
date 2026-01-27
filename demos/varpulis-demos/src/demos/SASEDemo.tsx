/**
 * SASE Security Demo Dashboard
 *
 * Real-time security event monitoring with pattern detection.
 * All data comes from Varpulis CEP engine via MQTT - no local simulation.
 */

import { AlertTriangle, Database, Filter, Shield, Wifi, WifiOff } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'
import PipelineGraph, { SASE_PIPELINE } from '../components/PipelineGraph'
import { useVarpulis, type VarpulisEvent } from '../hooks/useVarpulis'

const EVENT_TYPES = [
    { name: 'Login', icon: '🔐', color: 'text-blue-400' },
    { name: 'Transaction', icon: '💳', color: 'text-green-400' },
    { name: 'PasswordChange', icon: '🔑', color: 'text-yellow-400' },
    { name: 'Logout', icon: '🚪', color: 'text-slate-400' },
]

const PATTERNS = [
    { name: 'Account Takeover', vplTypes: ['potential_fraud_chain', 'large_transaction_after_login'], severity: 'critical' },
    { name: 'Impossible Travel', vplTypes: ['impossible_travel'], severity: 'critical' },
    { name: 'Fraud Pattern', vplTypes: ['risky_transaction', 'high_spending_24h'], severity: 'critical' },
    { name: 'High Velocity', vplTypes: ['high_velocity'], severity: 'warning' },
    { name: 'Money Mule', vplTypes: ['money_mule_suspect'], severity: 'critical' },
    { name: 'Correlated Activity', vplTypes: ['correlated_activity'], severity: 'warning' },
]

const STREAMS = [
    { name: 'Logins', source: 'Login', ops: [] },
    { name: 'Transactions', source: 'Transaction', ops: [] },
    { name: 'UserSessions', source: 'Logins', ops: ['partition by user_id'] },
    { name: 'GeoLocations', source: 'Logins', ops: ['extract country'] },
    { name: 'UserActivity', source: 'Transactions', ops: ['partition by user_id'] },
    { name: 'RiskScore', source: 'JOIN(Sessions, Activity)', ops: ['calculate risk'] },
]

export default function SASEDemo() {
    const { connected, mqttConnected, events, alerts } = useVarpulis()

    const [eventCounts, setEventCounts] = useState<Record<string, number>>({})
    const [streamCounts, setStreamCounts] = useState<Record<string, number>>({})
    const [patternMatches, setPatternMatches] = useState<Record<string, number>>({})
    const [recentEvents, setRecentEvents] = useState<VarpulisEvent[]>([])
    const [userActivity, setUserActivity] = useState<Record<string, { logins: number, transactions: number, amount: number }>>({})
    const [recentAlerts, setRecentAlerts] = useState<Array<{ type: string, message: string, user?: string, severity: string }>>([])

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
        const userId = String(alertData.user_id || '')
        const message = String(alertData.message || alertData.reason || '')

        // Map VPL alert_type to dashboard pattern names
        for (const pattern of PATTERNS) {
            if (pattern.vplTypes.includes(alertType)) {
                setPatternMatches(prev => ({ ...prev, [pattern.name]: (prev[pattern.name] || 0) + 1 }))
                setRecentAlerts(prev => [{
                    type: pattern.name,
                    message: message || `${pattern.name} detected`,
                    user: userId,
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
        const eventKey = `${latestEvent.type}-${latestEvent.timestamp}`
        if (eventKey === lastEventRef.current) return
        lastEventRef.current = eventKey

        // Update event counts
        const eventType = String(latestEvent.data?.event_type || latestEvent.type || 'Unknown')
        setEventCounts(prev => ({ ...prev, [eventType]: (prev[eventType] || 0) + 1 }))

        // Update recent events
        setRecentEvents(prev => [latestEvent, ...prev].slice(0, 20))

        // Update stream counts and user activity based on event type
        if (eventType === 'Login') {
            setStreamCounts(prev => ({
                ...prev,
                'Logins': (prev['Logins'] || 0) + 1,
                'UserSessions': (prev['UserSessions'] || 0) + 1,
                'GeoLocations': (prev['GeoLocations'] || 0) + 1,
            }))

            // Track user activity
            const userId = String(latestEvent.data?.user_id || 'unknown')
            setUserActivity(prev => ({
                ...prev,
                [userId]: {
                    logins: (prev[userId]?.logins || 0) + 1,
                    transactions: prev[userId]?.transactions || 0,
                    amount: prev[userId]?.amount || 0,
                }
            }))
        } else if (eventType === 'Transaction') {
            setStreamCounts(prev => ({
                ...prev,
                'Transactions': (prev['Transactions'] || 0) + 1,
                'UserActivity': (prev['UserActivity'] || 0) + 1,
            }))

            const amount = Number(latestEvent.data?.amount || 0)
            const userId = String(latestEvent.data?.user_id || 'unknown')
            setUserActivity(prev => ({
                ...prev,
                [userId]: {
                    logins: prev[userId]?.logins || 0,
                    transactions: (prev[userId]?.transactions || 0) + 1,
                    amount: (prev[userId]?.amount || 0) + amount,
                }
            }))
        } else if (eventType === 'PasswordChange') {
            setStreamCounts(prev => ({
                ...prev,
                'PasswordChanges': (prev['PasswordChanges'] || 0) + 1,
                'RiskScore': (prev['RiskScore'] || 0) + 1,
            }))
        } else if (eventType === 'Logout') {
            setStreamCounts(prev => ({
                ...prev,
                'Sessions': (prev['Sessions'] || 0) + 1,
            }))
        }
    }, [events])

    const totalEvents = Object.values(eventCounts).reduce((a, b) => a + b, 0)
    const totalPatterns = Object.values(patternMatches).reduce((a, b) => a + b, 0)

    return (
        <div className="p-4 space-y-4 max-w-[1600px] mx-auto">
            {/* Header */}
            <div className="flex items-center justify-between bg-slate-800/50 rounded-lg p-3 border border-slate-700">
                <div className="flex items-center gap-4">
                    <h1 className="text-xl font-bold text-white">SASE Security Demo</h1>
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
                    Security Event Pipeline
                </h2>
                <div className="text-xs text-slate-400 mb-2">
                    Events flow from left to right: Security Events → Streams → Derived Analysis → Pattern Detection
                </div>
                <PipelineGraph
                    nodes={SASE_PIPELINE.nodes}
                    edges={SASE_PIPELINE.edges}
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
                            Security Events
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
                        <h3 className="text-sm font-semibold text-slate-300 mb-2">Live Event Feed</h3>
                        <div className="space-y-1 text-xs font-mono">
                            {recentEvents.slice(0, 10).map((evt, i) => (
                                <div key={i} className="text-slate-400 truncate">
                                    <span className="text-blue-400">{String(evt.data?.event_type || evt.type)}</span>
                                    <span className="text-slate-600"> | </span>
                                    <span>{String(evt.data?.user_id || 'unknown')}</span>
                                    {evt.data?.amount ? <span className="text-green-400"> ${String(evt.data.amount)}</span> : null}
                                    {evt.data?.country ? <span className="text-yellow-400"> ({String(evt.data.country)})</span> : null}
                                </div>
                            ))}
                            {recentEvents.length === 0 && (
                                <div className="text-slate-500 italic">Waiting for events...</div>
                            )}
                        </div>
                    </div>
                </div>

                {/* Column 2: User Activity */}
                <div className="space-y-4">
                    <div className="bg-slate-800/50 rounded-lg p-4 border border-slate-700">
                        <h2 className="text-lg font-semibold text-white mb-3 flex items-center gap-2">
                            <Shield className="w-5 h-5 text-purple-400" />
                            User Activity
                        </h2>
                        <div className="space-y-2 max-h-[200px] overflow-y-auto">
                            {Object.entries(userActivity).slice(0, 6).map(([userId, data]) => (
                                <div key={userId} className="bg-slate-900/50 rounded p-2 border border-slate-600">
                                    <div className="flex items-center justify-between">
                                        <span className="text-sm font-medium text-purple-300">{userId}</span>
                                        <span className="text-xs text-slate-400">
                                            {data.logins} logins | {data.transactions} txns
                                        </span>
                                    </div>
                                    <div className="text-xs text-slate-500 mt-1">
                                        Total: <span className="text-green-400">${data.amount.toLocaleString()}</span>
                                    </div>
                                </div>
                            ))}
                            {Object.keys(userActivity).length === 0 && (
                                <div className="text-slate-500 text-sm italic">Waiting for user activity...</div>
                            )}
                        </div>
                    </div>

                    {/* Stream Counts */}
                    <div className="bg-slate-800/50 rounded-lg p-4 border border-slate-700">
                        <h3 className="text-sm font-semibold text-slate-300 mb-2">Stream Activity</h3>
                        <div className="space-y-2">
                            {STREAMS.map(stream => (
                                <div key={stream.name} className="bg-slate-900/50 rounded p-2 border border-slate-600">
                                    <div className="flex items-center justify-between">
                                        <span className="text-sm text-purple-300">{stream.name}</span>
                                        <span className="text-sm font-mono text-white">{streamCounts[stream.name] || 0}</span>
                                    </div>
                                    <div className="text-xs text-slate-500">{stream.ops.join(' → ') || 'passthrough'}</div>
                                </div>
                            ))}
                        </div>
                    </div>
                </div>

                {/* Column 3: Patterns & Alerts */}
                <div className="space-y-4">
                    <div className="bg-slate-800/50 rounded-lg p-4 border border-slate-700">
                        <h2 className="text-lg font-semibold text-white mb-3 flex items-center gap-2">
                            <AlertTriangle className="w-5 h-5 text-orange-400" />
                            Pattern Detection (from Varpulis)
                        </h2>
                        <div className="text-xs text-slate-400 mb-3">
                            Real patterns detected by Varpulis SASE+ engine
                        </div>
                        <div className="space-y-2">
                            {PATTERNS.map(pattern => {
                                const count = patternMatches[pattern.name] || 0
                                const isActive = count > 0
                                const severityColor = pattern.severity === 'critical' ? 'text-red-400' : 'text-yellow-400'
                                return (
                                    <div key={pattern.name} className={`bg-slate-900/50 rounded p-2 border ${isActive ? 'border-red-500/50' : 'border-slate-600'}`}>
                                        <div className="flex items-center justify-between">
                                            <span className={`text-sm font-medium ${isActive ? severityColor : 'text-slate-400'}`}>
                                                {pattern.severity === 'critical' ? '🔴' : '🟡'} {pattern.name}
                                            </span>
                                            <span className={`text-sm font-mono font-bold ${isActive ? severityColor : 'text-slate-500'}`}>{count}</span>
                                        </div>
                                        <div className="text-xs text-slate-500 mt-1 font-mono">{pattern.vplTypes.join(' | ')}</div>
                                    </div>
                                )
                            })}
                        </div>
                    </div>

                    {/* Alerts */}
                    <div className="bg-slate-800/50 rounded-lg p-4 border border-slate-700">
                        <h3 className="text-sm font-semibold text-red-400 mb-2">Active Alerts ({recentAlerts.length})</h3>
                        <div className="space-y-2 max-h-[200px] overflow-y-auto">
                            {recentAlerts.length === 0 ? (
                                <div className="text-xs text-slate-500">No alerts detected</div>
                            ) : (
                                recentAlerts.slice(0, 8).map((alert, i) => {
                                    const isCritical = alert.severity === 'critical' || alert.severity === 'high'
                                    const color = isCritical ? 'border-red-500/50 bg-red-500/10' : 'border-yellow-500/50 bg-yellow-500/10'
                                    const textColor = isCritical ? 'text-red-400' : 'text-yellow-400'
                                    return (
                                        <div key={i} className={`rounded p-2 border ${color}`}>
                                            <div className={`text-sm font-medium ${textColor}`}>
                                                {isCritical ? '🔴' : '🟡'} {alert.type}
                                            </div>
                                            {alert.user && <div className="text-xs text-slate-400">User: {alert.user}</div>}
                                            <div className="text-xs text-slate-300 mt-1">{alert.message}</div>
                                        </div>
                                    )
                                })
                            )}
                        </div>
                    </div>
                </div>
            </div>

            {/* Footer */}
            <div className="bg-slate-800/30 rounded-lg p-3 border border-slate-700/50 text-xs text-slate-500">
                <strong className="text-slate-400">How it works:</strong> Security events arrive via MQTT → Flow through Varpulis streams → SASE+ patterns are detected by the CEP engine → Alerts are generated.
                Run <code className="bg-slate-700 px-1 rounded">./start_demo.sh -d sase</code> to generate events automatically.
            </div>
        </div>
    )
}
