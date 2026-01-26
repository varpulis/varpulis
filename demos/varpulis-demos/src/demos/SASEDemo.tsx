import { AlertTriangle, Clock, CreditCard, Globe, MapPin, Pause, Play, RotateCcw, Settings, User } from 'lucide-react'
import { useEffect, useState } from 'react'

interface SecurityEvent {
    id: number
    type: 'login' | 'transaction' | 'logout' | 'access'
    userId: string
    ipAddress: string
    country: string
    amount?: number
    timestamp: Date
}

interface SecurityAlert {
    id: number
    pattern: string
    severity: 'low' | 'medium' | 'high' | 'critical'
    userId: string
    message: string
    events: SecurityEvent[]
    timestamp: Date
}

interface ActiveSession {
    userId: string
    loginTime: Date
    country: string
    transactions: number
    totalAmount: number
}

export default function SASEDemo() {
    const [isRunning, setIsRunning] = useState(false)
    const [events, setEvents] = useState<SecurityEvent[]>([])
    const [alerts, setAlerts] = useState<SecurityAlert[]>([])
    const [sessions, setSessions] = useState<Map<string, ActiveSession>>(new Map())
    const [eventCount, setEventCount] = useState(0)

    const [config, setConfig] = useState({
        largeTransactionThreshold: 10000,
        impossibleTravelMinutes: 60,
        highSpendingThreshold: 50000,
        sessionTimeout: 300
    })

    const countries = ['US', 'UK', 'DE', 'FR', 'JP', 'AU', 'BR', 'IN', 'CN', 'RU']
    const users = ['alice', 'bob', 'charlie', 'diana', 'eve']

    const generateEvent = (): SecurityEvent => {
        const types: ('login' | 'transaction' | 'logout' | 'access')[] = ['login', 'transaction', 'transaction', 'transaction', 'logout', 'access']
        const type = types[Math.floor(Math.random() * types.length)]
        const userId = users[Math.floor(Math.random() * users.length)]

        return {
            id: Date.now() + Math.random(),
            type,
            userId,
            ipAddress: `${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}`,
            country: countries[Math.floor(Math.random() * countries.length)],
            amount: type === 'transaction' ? Math.floor(Math.random() * 20000) + 100 : undefined,
            timestamp: new Date()
        }
    }

    useEffect(() => {
        if (!isRunning) return

        const interval = setInterval(() => {
            const event = generateEvent()
            setEvents(prev => [event, ...prev].slice(0, 50))
            setEventCount(prev => prev + 1)

            // Pattern detection
            if (event.type === 'login') {
                // Track session
                setSessions(prev => {
                    const newSessions = new Map(prev)
                    newSessions.set(event.userId, {
                        userId: event.userId,
                        loginTime: event.timestamp,
                        country: event.country,
                        transactions: 0,
                        totalAmount: 0
                    })
                    return newSessions
                })
            }

            if (event.type === 'transaction' && event.amount) {
                const session = sessions.get(event.userId)

                // Pattern 1: Large transaction after login
                if (session && event.amount > config.largeTransactionThreshold) {
                    const timeSinceLogin = (event.timestamp.getTime() - session.loginTime.getTime()) / 1000
                    if (timeSinceLogin < 60) { // Within 1 minute of login
                        setAlerts(prev => [{
                            id: Date.now(),
                            pattern: 'large_transaction_after_login',
                            severity: 'high' as const,
                            userId: event.userId,
                            message: `Large transaction ($${event.amount!.toLocaleString()}) within ${timeSinceLogin.toFixed(0)}s of login`,
                            events: [event],
                            timestamp: new Date()
                        }, ...prev].slice(0, 30))
                    }
                }

                // Pattern 2: Impossible travel
                if (session && session.country !== event.country) {
                    setAlerts(prev => [{
                        id: Date.now(),
                        pattern: 'impossible_travel',
                        severity: 'critical' as const,
                        userId: event.userId,
                        message: `Activity from ${event.country} but logged in from ${session.country}`,
                        events: [event],
                        timestamp: new Date()
                    }, ...prev].slice(0, 30))
                }

                // Update session
                setSessions(prev => {
                    const newSessions = new Map(prev)
                    const existing = newSessions.get(event.userId)
                    if (existing) {
                        newSessions.set(event.userId, {
                            ...existing,
                            transactions: existing.transactions + 1,
                            totalAmount: existing.totalAmount + (event.amount || 0)
                        })

                        // Pattern 3: High spending
                        if (existing.totalAmount + (event.amount || 0) > config.highSpendingThreshold) {
                            setAlerts(prev => [{
                                id: Date.now(),
                                pattern: 'high_spending',
                                severity: 'medium' as const,
                                userId: event.userId,
                                message: `Total spending ($${(existing.totalAmount + (event.amount || 0)).toLocaleString()}) exceeds threshold`,
                                events: [event],
                                timestamp: new Date()
                            }, ...prev].slice(0, 30))
                        }
                    }
                    return newSessions
                })
            }

            if (event.type === 'logout') {
                setSessions(prev => {
                    const newSessions = new Map(prev)
                    newSessions.delete(event.userId)
                    return newSessions
                })
            }
        }, 800)

        return () => clearInterval(interval)
    }, [isRunning, sessions, config])

    const reset = () => {
        setIsRunning(false)
        setEvents([])
        setAlerts([])
        setSessions(new Map())
        setEventCount(0)
    }

    const getSeverityColor = (severity: string) => {
        switch (severity) {
            case 'critical': return 'bg-red-500/20 border-red-500/50 text-red-400'
            case 'high': return 'bg-orange-500/20 border-orange-500/50 text-orange-400'
            case 'medium': return 'bg-yellow-500/20 border-yellow-500/50 text-yellow-400'
            default: return 'bg-blue-500/20 border-blue-500/50 text-blue-400'
        }
    }

    const getEventIcon = (type: string) => {
        switch (type) {
            case 'login': return <User className="w-4 h-4 text-emerald-400" />
            case 'logout': return <User className="w-4 h-4 text-slate-400" />
            case 'transaction': return <CreditCard className="w-4 h-4 text-blue-400" />
            case 'access': return <Globe className="w-4 h-4 text-purple-400" />
            default: return <Clock className="w-4 h-4 text-slate-400" />
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
                        {isRunning ? 'Pause' : 'Start'} Stream
                    </button>
                    <button onClick={reset} className="flex items-center gap-2 px-4 py-2 rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600">
                        <RotateCcw className="w-4 h-4" />
                        Reset
                    </button>
                </div>
                <div className="flex items-center gap-4 text-sm text-slate-400">
                    <span>Events: <span className="text-white font-mono">{eventCount}</span></span>
                    <span>Alerts: <span className="text-white font-mono">{alerts.length}</span></span>
                    <span>Sessions: <span className="text-white font-mono">{sessions.size}</span></span>
                </div>
            </div>

            {/* Stats Bar */}
            <div className="grid grid-cols-4 gap-4 mb-6">
                <div className="card text-center">
                    <div className="text-3xl font-bold text-white">{events.filter(e => e.type === 'login').length}</div>
                    <div className="text-sm text-slate-400">Logins</div>
                </div>
                <div className="card text-center">
                    <div className="text-3xl font-bold text-blue-400">{events.filter(e => e.type === 'transaction').length}</div>
                    <div className="text-sm text-slate-400">Transactions</div>
                </div>
                <div className="card text-center">
                    <div className="text-3xl font-bold text-yellow-400">{alerts.filter(a => a.severity === 'high' || a.severity === 'medium').length}</div>
                    <div className="text-sm text-slate-400">Warnings</div>
                </div>
                <div className="card text-center">
                    <div className="text-3xl font-bold text-red-400">{alerts.filter(a => a.severity === 'critical').length}</div>
                    <div className="text-sm text-slate-400">Critical</div>
                </div>
            </div>

            <div className="grid lg:grid-cols-3 gap-6">
                {/* Event Stream */}
                <div className="card">
                    <h3 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
                        <Clock className="w-5 h-5 text-slate-400" />
                        Event Stream
                    </h3>
                    <div className="space-y-2 max-h-96 overflow-y-auto">
                        {events.length === 0 ? (
                            <p className="text-slate-400 text-sm text-center py-4">No events yet. Start the stream.</p>
                        ) : (
                            events.slice(0, 20).map(event => (
                                <div key={event.id} className="flex items-center gap-3 p-2 bg-slate-700/50 rounded-lg text-sm">
                                    {getEventIcon(event.type)}
                                    <div className="flex-1 min-w-0">
                                        <div className="flex items-center justify-between">
                                            <span className="font-medium text-white">{event.userId}</span>
                                            <span className="text-xs text-slate-500">{event.timestamp.toLocaleTimeString()}</span>
                                        </div>
                                        <div className="flex items-center gap-2 text-xs text-slate-400">
                                            <span className="capitalize">{event.type}</span>
                                            {event.amount && <span className="text-emerald-400">${event.amount.toLocaleString()}</span>}
                                            <span className="flex items-center gap-1"><MapPin className="w-3 h-3" />{event.country}</span>
                                        </div>
                                    </div>
                                </div>
                            ))
                        )}
                    </div>
                </div>

                {/* Alerts */}
                <div className="card">
                    <h3 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
                        <AlertTriangle className="w-5 h-5 text-yellow-400" />
                        Security Alerts
                    </h3>
                    <div className="space-y-2 max-h-96 overflow-y-auto">
                        {alerts.length === 0 ? (
                            <p className="text-slate-400 text-sm text-center py-4">No alerts detected.</p>
                        ) : (
                            alerts.slice(0, 15).map(alert => (
                                <div key={alert.id} className={`p-3 rounded-lg border ${getSeverityColor(alert.severity)}`}>
                                    <div className="flex items-center justify-between mb-1">
                                        <span className="text-sm font-medium capitalize">{alert.pattern.replace(/_/g, ' ')}</span>
                                        <span className={`text-xs px-2 py-0.5 rounded ${getSeverityColor(alert.severity)}`}>
                                            {alert.severity.toUpperCase()}
                                        </span>
                                    </div>
                                    <p className="text-xs text-slate-300 mb-1">{alert.message}</p>
                                    <div className="flex items-center justify-between text-xs text-slate-500">
                                        <span>User: {alert.userId}</span>
                                        <span>{alert.timestamp.toLocaleTimeString()}</span>
                                    </div>
                                </div>
                            ))
                        )}
                    </div>
                </div>

                {/* Active Sessions & Config */}
                <div className="space-y-6">
                    {/* Active Sessions */}
                    <div className="card">
                        <h3 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
                            <User className="w-5 h-5 text-emerald-400" />
                            Active Sessions
                        </h3>
                        <div className="space-y-2">
                            {sessions.size === 0 ? (
                                <p className="text-slate-400 text-sm text-center py-4">No active sessions.</p>
                            ) : (
                                Array.from(sessions.values()).map(session => (
                                    <div key={session.userId} className="flex items-center justify-between p-2 bg-slate-700/50 rounded-lg">
                                        <div>
                                            <span className="font-medium text-white">{session.userId}</span>
                                            <div className="text-xs text-slate-400 flex items-center gap-2">
                                                <MapPin className="w-3 h-3" />{session.country}
                                                <span>•</span>
                                                <span>{session.transactions} txns</span>
                                            </div>
                                        </div>
                                        <div className="text-right">
                                            <div className="text-sm font-medium text-emerald-400">${session.totalAmount.toLocaleString()}</div>
                                            <div className="text-xs text-slate-500">{session.loginTime.toLocaleTimeString()}</div>
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
                            Pattern Thresholds
                        </h3>
                        <div className="space-y-3">
                            <div>
                                <label className="block text-sm text-slate-400 mb-1">Large Transaction ($)</label>
                                <input
                                    type="number"
                                    value={config.largeTransactionThreshold}
                                    onChange={e => setConfig(prev => ({ ...prev, largeTransactionThreshold: Number(e.target.value) }))}
                                    className="input-field w-full"
                                />
                            </div>
                            <div>
                                <label className="block text-sm text-slate-400 mb-1">High Spending Limit ($)</label>
                                <input
                                    type="number"
                                    value={config.highSpendingThreshold}
                                    onChange={e => setConfig(prev => ({ ...prev, highSpendingThreshold: Number(e.target.value) }))}
                                    className="input-field w-full"
                                />
                            </div>
                        </div>
                        <div className="mt-4 p-3 bg-slate-900 rounded-lg">
                            <p className="text-xs text-slate-400 font-mono">
                                stream LargeTransactionAfterLogin<br />
                                &nbsp;&nbsp;from Login as l -&gt; Transaction as t<br />
                                &nbsp;&nbsp;where t.user_id == l.user_id<br />
                                &nbsp;&nbsp;&nbsp;&nbsp;and t.amount &gt; {config.largeTransactionThreshold}<br />
                                &nbsp;&nbsp;within 1 minute<br />
                                &nbsp;&nbsp;emit {'{'}user_id, amount, alert: "suspicious"{'}'}
                            </p>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    )
}
