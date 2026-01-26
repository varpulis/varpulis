import { Building2, Play, Settings, Shield, TrendingUp, Zap } from 'lucide-react'
import { useState } from 'react'
import FinancialDemo from './demos/FinancialDemo'
import HVACDemo from './demos/HVACDemo'
import SASEDemo from './demos/SASEDemo'

type DemoType = 'hvac' | 'financial' | 'sase' | null

function App() {
  const [activeDemo, setActiveDemo] = useState<DemoType>(null)

  const demos = [
    {
      id: 'hvac' as const,
      title: 'HVAC Monitoring',
      description: 'Real-time building automation with temperature anomaly detection, energy optimization, and predictive maintenance alerts.',
      icon: Building2,
      color: 'from-emerald-500 to-teal-600',
      features: ['Temperature Zones', 'Energy Metrics', 'Anomaly Detection', 'Predictive Alerts']
    },
    {
      id: 'financial' as const,
      title: 'Financial Markets',
      description: 'Technical analysis with moving averages, MACD, RSI indicators, and trading signal generation.',
      icon: TrendingUp,
      color: 'from-blue-500 to-indigo-600',
      features: ['Price Charts', 'SMA/EMA', 'Golden/Death Cross', 'Volume Analysis']
    },
    {
      id: 'sase' as const,
      title: 'SASE Security',
      description: 'Security pattern detection with login sequences, fraud chains, impossible travel, and behavioral analysis.',
      icon: Shield,
      color: 'from-purple-500 to-pink-600',
      features: ['Pattern Matching', 'Fraud Detection', 'Session Tracking', 'Threat Timeline']
    }
  ]

  if (activeDemo) {
    return (
      <div className="min-h-screen bg-slate-900">
        <header className="bg-slate-800 border-b border-slate-700 px-6 py-4">
          <div className="flex items-center justify-between max-w-7xl mx-auto">
            <div className="flex items-center gap-4">
              <button
                onClick={() => setActiveDemo(null)}
                className="text-slate-400 hover:text-white transition-colors"
              >
                ← Back
              </button>
              <div className="flex items-center gap-2">
                <Zap className="w-6 h-6 text-primary-400" />
                <span className="text-xl font-bold text-white">Varpulis</span>
              </div>
            </div>
            <div className="flex items-center gap-3">
              <span className="text-slate-400 text-sm">
                {demos.find(d => d.id === activeDemo)?.title}
              </span>
            </div>
          </div>
        </header>

        {activeDemo === 'hvac' && <HVACDemo />}
        {activeDemo === 'financial' && <FinancialDemo />}
        {activeDemo === 'sase' && <SASEDemo />}
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-slate-900 py-12 px-6">
      <div className="max-w-6xl mx-auto">
        {/* Hero */}
        <div className="text-center mb-16">
          <div className="flex items-center justify-center gap-3 mb-6">
            <Zap className="w-12 h-12 text-primary-400" />
            <h1 className="text-5xl font-bold text-white">Varpulis</h1>
          </div>
          <p className="text-xl text-slate-400 max-w-2xl mx-auto mb-8">
            High-performance Complex Event Processing engine for real-time stream analytics.
            Detect patterns, correlate events, and generate alerts in milliseconds.
          </p>
          <div className="flex items-center justify-center gap-4 text-sm text-slate-500">
            <span className="flex items-center gap-1">
              <span className="w-2 h-2 bg-emerald-500 rounded-full"></span>
              Rust-powered
            </span>
            <span className="flex items-center gap-1">
              <span className="w-2 h-2 bg-blue-500 rounded-full"></span>
              SASE+ Patterns
            </span>
            <span className="flex items-center gap-1">
              <span className="w-2 h-2 bg-purple-500 rounded-full"></span>
              Sub-ms Latency
            </span>
          </div>
        </div>

        {/* Demo Cards */}
        <div className="grid md:grid-cols-3 gap-6 mb-16">
          {demos.map((demo) => {
            const Icon = demo.icon
            return (
              <div
                key={demo.id}
                className="bg-slate-800 rounded-2xl border border-slate-700 overflow-hidden hover:border-slate-600 transition-all group cursor-pointer"
                onClick={() => setActiveDemo(demo.id)}
              >
                <div className={`h-32 bg-gradient-to-br ${demo.color} flex items-center justify-center`}>
                  <Icon className="w-16 h-16 text-white/90" />
                </div>
                <div className="p-6">
                  <h3 className="text-xl font-semibold text-white mb-2">{demo.title}</h3>
                  <p className="text-slate-400 text-sm mb-4">{demo.description}</p>
                  <div className="flex flex-wrap gap-2 mb-4">
                    {demo.features.map((f) => (
                      <span key={f} className="text-xs bg-slate-700 text-slate-300 px-2 py-1 rounded">
                        {f}
                      </span>
                    ))}
                  </div>
                  <button className="w-full flex items-center justify-center gap-2 bg-slate-700 hover:bg-slate-600 text-white font-medium py-2.5 rounded-lg transition-colors group-hover:bg-primary-600">
                    <Play className="w-4 h-4" />
                    Launch Demo
                  </button>
                </div>
              </div>
            )
          })}
        </div>

        {/* Features */}
        <div className="bg-slate-800/50 rounded-2xl border border-slate-700 p-8">
          <h2 className="text-2xl font-bold text-white mb-6 text-center">Why Varpulis?</h2>
          <div className="grid md:grid-cols-3 gap-8">
            <div className="text-center">
              <div className="w-12 h-12 bg-emerald-500/10 rounded-xl flex items-center justify-center mx-auto mb-4">
                <Zap className="w-6 h-6 text-emerald-400" />
              </div>
              <h3 className="text-lg font-semibold text-white mb-2">Blazing Fast</h3>
              <p className="text-slate-400 text-sm">Sub-millisecond pattern matching with Rust's zero-cost abstractions</p>
            </div>
            <div className="text-center">
              <div className="w-12 h-12 bg-blue-500/10 rounded-xl flex items-center justify-center mx-auto mb-4">
                <Settings className="w-6 h-6 text-blue-400" />
              </div>
              <h3 className="text-lg font-semibold text-white mb-2">Declarative Rules</h3>
              <p className="text-slate-400 text-sm">Express complex patterns with simple VarpulisQL syntax</p>
            </div>
            <div className="text-center">
              <div className="w-12 h-12 bg-purple-500/10 rounded-xl flex items-center justify-center mx-auto mb-4">
                <Shield className="w-6 h-6 text-purple-400" />
              </div>
              <h3 className="text-lg font-semibold text-white mb-2">Production Ready</h3>
              <p className="text-slate-400 text-sm">Battle-tested SASE+ algorithm with attention windows</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

export default App
