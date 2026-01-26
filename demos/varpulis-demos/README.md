# ⚡ Varpulis Interactive Demos

Interactive dashboards showcasing the power of Varpulis Complex Event Processing engine.

![Varpulis](https://img.shields.io/badge/Varpulis-CEP%20Engine-blue)
![React](https://img.shields.io/badge/React-18-61dafb)
![TypeScript](https://img.shields.io/badge/TypeScript-5-3178c6)

## 🚀 Quick Start

```bash
# Install dependencies
npm install

# Start development server
npm run dev

# Build for production
npm run build
```

Open [http://localhost:5173](http://localhost:5173) to view the demos.

## 📊 Available Demos

### 1. 🏢 HVAC Monitoring

Real-time building automation system monitoring temperature zones, humidity levels, and energy consumption.

**Features:**
- Live temperature visualization across multiple zones
- Anomaly detection with configurable thresholds
- Energy consumption tracking
- Real-time alert generation

**VPL Pattern Example:**
```vpl
stream TemperatureAnomalies from TemperatureReading
  where value > 28 or value < 15
  emit { zone, value, alert_type: "temperature_anomaly" }
```

### 2. 📈 Financial Markets

Technical analysis dashboard with moving averages, crossover detection, and trading signals.

**Features:**
- Real-time price charts with SMA overlays
- Golden Cross / Death Cross detection
- Volume spike alerts
- Configurable indicator periods

**VPL Pattern Example:**
```vpl
stream GoldenCross from MarketTick
  where sma(20) > sma(50)
    and prev(sma(20)) <= prev(sma(50))
  emit { symbol, signal: "buy" }
```

### 3. 🛡️ SASE Security Patterns

Security event correlation with fraud detection, session tracking, and behavioral analysis.

**Features:**
- Event stream visualization
- Pattern-based alert generation
- Active session monitoring
- Impossible travel detection

**VPL Pattern Example:**
```vpl
stream LargeTransactionAfterLogin
  from Login as l -> Transaction as t
  where t.user_id == l.user_id
    and t.amount > 10000
  within 1 minute
  emit { user_id, amount, alert: "suspicious" }
```

## 🎮 How to Use

1. **Select a Demo** - Click on any demo card from the home screen
2. **Start Simulation** - Click the "Start" button to begin event generation
3. **Watch Patterns** - Observe real-time charts and alert generation
4. **Adjust Config** - Modify thresholds to see how patterns change
5. **Analyze Alerts** - Review generated alerts in the alerts panel

## 🛠️ Configuration

Each demo includes a configuration panel where you can adjust:

- **Thresholds** - Alert trigger values
- **Window sizes** - Time windows for aggregations
- **Pattern parameters** - Sequence timing constraints

Changes take effect immediately on the simulation.

## 📁 Project Structure

```
src/
├── App.tsx              # Main app with demo launcher
├── demos/
│   ├── HVACDemo.tsx     # HVAC monitoring dashboard
│   ├── FinancialDemo.tsx # Financial markets dashboard
│   └── SASEDemo.tsx     # Security patterns dashboard
└── index.css            # Tailwind styles
```

## 🔗 Related Resources

- [Varpulis Documentation](../docs/)
- [VPL Language Reference](../docs/language/)
- [Example VPL Files](../examples/)
- [E2E Tests](../tests/e2e/)

## 📝 License

MIT License - See [LICENSE](../../LICENSE) for details.
