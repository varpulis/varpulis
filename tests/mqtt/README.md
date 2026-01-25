# Varpulis MQTT Test Infrastructure

Infrastructure de test avec injection d'événements via MQTT pour valider les patterns CEP.

## Architecture

```
┌─────────────────┐     MQTT      ┌──────────────────┐
│   Simulator     │──────────────▶│    Mosquitto     │
│   (Python)      │               │    (Broker)      │
└─────────────────┘               └────────┬─────────┘
                                           │
                                           ▼
                                  ┌──────────────────┐
                                  │    Varpulis      │
                                  │    Runtime       │
                                  └──────────────────┘
```

## Démarrage Rapide

### 1. Démarrer Mosquitto

```bash
cd tests/mqtt
docker-compose up -d
```

### 2. Installer les dépendances Python

```bash
pip install -r requirements.txt
```

### 3. Lancer une simulation

```bash
# Scénario fraude - 100 events/sec pendant 60s
python simulator.py --scenario fraud --rate 100 --duration 60

# Scénario trading - mode burst (max throughput)
python simulator.py --scenario trading --burst --duration 30

# Scénario IoT - 50 capteurs
python simulator.py --scenario iot --sensors 50 --rate 200
```

## Scénarios Disponibles

### 🔐 Fraud Detection (`--scenario fraud`)

Simule des patterns de fraude bancaire:
- **Account Takeover**: Login → PasswordChange → Large Transaction
- **Impossible Travel**: Logins depuis différents pays en < 1h
- **High Velocity**: 10+ transactions rapides

Options:
- `--users N`: Nombre d'utilisateurs simulés (défaut: 100)

### 📈 Trading (`--scenario trading`)

Données de marché haute fréquence:
- Market ticks avec prix, volume, bid/ask
- Mouvements de prix (random walk)
- Pics de volume occasionnels

### 🌡️ IoT (`--scenario iot`)

Capteurs IoT avec anomalies:
- Lectures température, humidité, pression
- Pics de température
- Défaillances capteur

Options:
- `--sensors N`: Nombre de capteurs (défaut: 50)

## Topics MQTT

Les événements sont publiés sur:
```
varpulis/events/{EventType}
```

Exemples:
- `varpulis/events/Login`
- `varpulis/events/Transaction`
- `varpulis/events/MarketTick`
- `varpulis/events/SensorReading`

## Format des Messages

```json
{
  "event_type": "Transaction",
  "timestamp": "2024-01-15T10:30:00.123456",
  "data": {
    "user_id": "user_0042",
    "amount": 1234.56,
    "merchant": "Amazon",
    "category": "electronics"
  }
}
```

## Validation des Patterns

Le simulateur track les patterns attendus:

```bash
$ python simulator.py --scenario fraud --duration 30

Simulation Summary
┏━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Metric           ┃ Value         ┃
┡━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
│ Events Sent      │ 3,000         │
│ Expected Patterns│ 42            │
└──────────────────┴───────────────┘

Expected Pattern Matches:
  - AccountTakeover: 15
  - ImpossibleTravel: 12
  - HighVelocity: 15
```

## Tests de Performance

### Throughput Maximum

```bash
python simulator.py --scenario trading --burst --duration 60
```

### Test de Charge

```bash
# Terminal 1: Fraud à 500/sec
python simulator.py -s fraud -r 500 -d 300

# Terminal 2: Trading à 1000/sec
python simulator.py -s trading -r 1000 -d 300

# Terminal 3: IoT à 200/sec
python simulator.py -s iot -r 200 -d 300
```

## Debugging

### Écouter les messages MQTT

```bash
mosquitto_sub -h localhost -t "varpulis/events/#" -v
```

### Logs Mosquitto

```bash
docker-compose logs -f mosquitto
```

## Intégration avec Varpulis

```bash
# Lancer Varpulis avec connecteur MQTT
varpulis run examples/sase_patterns.vpl --mqtt localhost:1883

# Dans un autre terminal, lancer le simulateur
python tests/mqtt/simulator.py --scenario fraud --rate 100
```
