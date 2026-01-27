# Varpulis Interactive Demos

Démonstrations interactives du moteur CEP Varpulis avec **vraie intégration** MQTT.

## Architecture

```
┌─────────────────────┐     MQTT      ┌──────────────────┐     MQTT      ┌─────────────────────┐
│  Event Injector     │──────────────▶│    Mosquitto     │◀──────────────│    Varpulis         │
│  (Python scripts)   │               │    (Broker)      │               │    Runtime          │
└─────────────────────┘               └────────┬─────────┘               └──────────┬──────────┘
                                               │                                    │
                                               │ varpulis/events/#                  │ varpulis/alerts/#
                                               │                                    │ varpulis/dashboard/#
                                               ▼                                    │
                                      ┌──────────────────┐                          │
                                      │  Dashboard       │◀─────────────────────────┘
                                      │  Server (WS)     │
                                      └────────┬─────────┘
                                               │ WebSocket
                                               ▼
                                      ┌──────────────────┐
                                      │  React Dashboard │
                                      │  (Browser)       │
                                      └──────────────────┘
```

## Démarrage Rapide

### 1. Démarrer l'infrastructure

```bash
cd demos

# Démarrer Mosquitto + Dashboard Server
docker-compose up -d

# Ou manuellement:
# Terminal 1: Mosquitto
docker run -it -p 1883:1883 -p 9001:9001 eclipse-mosquitto:2

# Terminal 2: Dashboard Server
cd varpulis-demos/server && npm install && npm start
```

### 2. Lancer Varpulis

**Option A : Mode simulation avec fichier événements**
```bash
# HVAC Demo
varpulis simulate -p examples/hvac_demo.vpl -e tests/scenarios/hvac_monitoring.evt -v

# Financial Markets Demo  
varpulis simulate -p examples/financial_markets.vpl -e tests/scenarios/financial_markets.evt -v

# SASE Security Demo
varpulis simulate -p examples/sase_patterns.vpl -e tests/scenarios/sase_patterns.evt -v
```

**Option B : Mode serveur WebSocket**
```bash
varpulis server -p 9000
```

### 3. Injecter des événements

```bash
# Installer les dépendances Python
pip install -r demos/requirements.txt

# HVAC: 10 events/sec pendant 60s, anomalie après 30s
python demos/hvac/inject_events.py --rate 10 --duration 60 --anomaly-after 30

# Financial: 100 ticks/sec avec patterns
python demos/financial/inject_events.py --rate 100 --duration 60

# SASE: 50 events/sec avec attaques toutes les 15s
python demos/sase/inject_events.py --rate 50 --duration 60 --attack-interval 15
```

### 4. Ouvrir le Dashboard

```bash
cd demos/varpulis-demos
npm install
npm run dev
```

Ouvrir http://localhost:5173

## Démos Disponibles

### 🏢 HVAC Monitoring

**Fichier principal:** `demos/hvac/main.vpl`

Surveillance en temps réel d'un système HVAC:
- Lectures température par zone
- Détection d'anomalies thermiques
- Alertes humidité
- Suivi consommation énergétique

```bash
# Lancer la démo complète
varpulis run demos/hvac/main.vpl --mqtt localhost:1883 &
python demos/hvac/inject_events.py --rate 10 --duration 120
```

### 📈 Financial Markets

**Fichier principal:** `demos/financial/main.vpl`

Analyse technique temps réel:
- Prix et volumes par symbole
- Moyennes mobiles (SMA)
- Golden Cross / Death Cross
- Alertes pics de volume

```bash
# Lancer la démo complète
varpulis run demos/financial/main.vpl --mqtt localhost:1883 &
python demos/financial/inject_events.py --rate 100 --duration 120
```

### 🛡️ SASE Security

**Fichier principal:** `demos/sase/main.vpl`

Détection de patterns de sécurité:
- Account Takeover (Login → PasswordChange → LargeTransaction)
- Impossible Travel (logins depuis différents pays)
- High Velocity Spending (transactions rapides)

```bash
# Lancer la démo complète
varpulis run demos/sase/main.vpl --mqtt localhost:1883 &
python demos/sase/inject_events.py --rate 50 --duration 120 --attack-interval 15
```

## Structure des Fichiers

```
demos/
├── docker-compose.yml          # Stack complète (Mosquitto + Server)
├── requirements.txt            # Dépendances Python
├── mosquitto/
│   └── mosquitto.conf          # Config MQTT broker
├── hvac/
│   ├── main.vpl                # Config MQTT + import hvac_demo.vpl
│   └── inject_events.py        # Simulateur événements HVAC
├── financial/
│   ├── main.vpl                # Config MQTT + import financial_markets.vpl
│   └── inject_events.py        # Simulateur market ticks
├── sase/
│   ├── main.vpl                # Config MQTT + import sase_patterns.vpl
│   └── inject_events.py        # Simulateur événements sécurité
└── varpulis-demos/
    ├── src/
    │   ├── App.tsx             # Launcher des démos
    │   ├── hooks/
    │   │   └── useVarpulis.ts  # Hook WebSocket pour données temps réel
    │   └── demos/
    │       ├── HVACDemo.tsx
    │       ├── FinancialDemo.tsx
    │       └── SASEDemo.tsx
    └── server/
        ├── index.js            # Bridge MQTT → WebSocket
        └── Dockerfile
```

## Topics MQTT

### Input (événements bruts)
```
varpulis/events/TemperatureReading
varpulis/events/HumidityReading
varpulis/events/EnergyReading
varpulis/events/MarketTick
varpulis/events/Trade
varpulis/events/Login
varpulis/events/Transaction
varpulis/events/PasswordChange
```

### Output (résultats Varpulis)
```
varpulis/alerts/#                    # Alertes générées
varpulis/dashboard/zones             # Résumé zones HVAC
varpulis/dashboard/energy            # Consommation énergie
varpulis/dashboard/prices            # Prix agrégés
varpulis/dashboard/signals           # Signaux trading
varpulis/dashboard/sessions          # Sessions actives
varpulis/dashboard/security_alerts   # Alertes sécurité
```

## Debugging

### Écouter les messages MQTT

```bash
# Tous les événements
mosquitto_sub -h localhost -t "varpulis/#" -v

# Seulement les alertes
mosquitto_sub -h localhost -t "varpulis/alerts/#" -v

# Seulement le dashboard
mosquitto_sub -h localhost -t "varpulis/dashboard/#" -v
```

### Logs Varpulis

```bash
RUST_LOG=info varpulis run demos/hvac/main.vpl --mqtt localhost:1883
```

## Configuration VPL avec MQTT

Les fichiers `main.vpl` de chaque démo définissent la configuration MQTT:

```vpl
config mqtt {
    broker: "localhost",
    port: 1883,
    client_id: "varpulis-demo",
    input_topic: "varpulis/events/#",
    output_topic: "varpulis/alerts"
}

// Import de la logique métier
import "../../examples/hvac_demo.vpl"

// Streams spécifiques au dashboard
stream DashboardAlerts from TemperatureAnomaly
    emit to "varpulis/dashboard/alerts"
```

## Prérequis

- **Varpulis CLI** compilé avec feature `mqtt`:
  ```bash
  cargo build --release --features mqtt
  ```
- **Python 3.8+** avec `paho-mqtt`, `click`, `faker`, `rich`
- **Node.js 18+** pour le dashboard server
- **Docker** (optionnel) pour Mosquitto
