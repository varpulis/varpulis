# Cas d'usage MVP : Supervision HVAC Bâtiment

## Contexte

Supervision d'un bâtiment intelligent avec :
- Système HVAC (Heating, Ventilation, Air Conditioning)
- Capteurs de température, humidité, pression
- Détection d'anomalies et prédiction de pannes

## Architecture du bâtiment simulé

```
┌─────────────────────────────────────────────────────────┐
│                    BÂTIMENT ALPHA                       │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │   Zone A    │  │   Zone B    │  │   Zone C    │     │
│  │  (Bureaux)  │  │  (Serveurs) │  │  (Accueil)  │     │
│  │             │  │             │  │             │     │
│  │ T: 21-23°C  │  │ T: 18-20°C  │  │ T: 20-24°C  │     │
│  │ H: 40-60%   │  │ H: 45-55%   │  │ H: 40-60%   │     │
│  └─────────────┘  └─────────────┘  └─────────────┘     │
│         │                │                │             │
│         └────────────────┼────────────────┘             │
│                          │                              │
│                   ┌──────▼──────┐                       │
│                   │  CTA/HVAC   │                       │
│                   │  Centrale   │                       │
│                   └─────────────┘                       │
└─────────────────────────────────────────────────────────┘
```

## Événements du domaine

### TemperatureReading
Lecture de température d'un capteur.

```varpulis
event TemperatureReading:
    sensor_id: str          # "zone_a_temp_01"
    zone: str               # "zone_a", "zone_b", "zone_c"
    value: float            # Température en °C
    timestamp: timestamp
```

### HumidityReading
Lecture d'humidité relative.

```varpulis
event HumidityReading:
    sensor_id: str
    zone: str
    value: float            # Humidité en %
    timestamp: timestamp
```

### HVACStatus
État du système HVAC.

```varpulis
event HVACStatus:
    unit_id: str            # "cta_main", "cta_backup"
    mode: str               # "heating", "cooling", "ventilation", "off"
    power_consumption: float # kW
    fan_speed: int          # RPM
    compressor_pressure: float
    timestamp: timestamp
```

### EnergyReading
Consommation électrique.

```varpulis
event EnergyReading:
    meter_id: str
    zone: str
    power_kw: float
    timestamp: timestamp
```

## Streams et patterns à détecter

### 1. Monitoring de base par zone

```varpulis
# Agrégation des températures par zone
stream ZoneTemperatures = TemperatureReading
    .partition_by(zone)
    .window(5m, sliding: 1m)
    .aggregate(
        zone: first(zone),
        avg_temp: avg(value),
        min_temp: min(value),
        max_temp: max(value),
        readings_count: count()
    )
```

### 2. Détection d'anomalies de température

```varpulis
# Seuils par zone
const TEMP_THRESHOLDS = {
    "zone_a": { "min": 19.0, "max": 25.0 },
    "zone_b": { "min": 16.0, "max": 22.0 },  # Salle serveurs, plus strict
    "zone_c": { "min": 18.0, "max": 26.0 }
}

stream TemperatureAnomaly = ZoneTemperatures
    .where(
        avg_temp < TEMP_THRESHOLDS[zone].min or
        avg_temp > TEMP_THRESHOLDS[zone].max
    )
    .emit(
        alert_type: "temperature_anomaly",
        severity: if zone == "zone_b" then "critical" else "warning",
        zone: zone,
        current_temp: avg_temp,
        threshold_min: TEMP_THRESHOLDS[zone].min,
        threshold_max: TEMP_THRESHOLDS[zone].max,
        timestamp: now()
    )
```

### 3. Corrélation température/consommation HVAC

```varpulis
# Jointure température + consommation HVAC
stream TempEnergyCorrelation = join(
    stream Temps from ZoneTemperatures,
    stream HVAC from HVACStatus
        on true  # Jointure temporelle pure
)
.window(15m)
.aggregate(
    avg_temp: avg(Temps.avg_temp),
    avg_power: avg(HVAC.power_consumption),
    efficiency: avg(Temps.avg_temp) / avg(HVAC.power_consumption)
)
```

### 4. Détection de dégradation HVAC (pattern complexe)

```varpulis
# Utilise l'attention pour détecter des corrélations anormales
stream HVACDegradation = HVACStatus
    .attention_window(
        duration: 30m,
        heads: 4,
        embedding: "rule_based"
    )
    .pattern(
        degradation: events =>
            # Tendance à la hausse de la consommation
            let power_trend = events
                .map(e => e.power_consumption)
                .linear_trend()
            
            # Tendance à la baisse de l'efficacité (pression compresseur)
            let pressure_trend = events
                .map(e => e.compressor_pressure)
                .linear_trend()
            
            # Corrélation anormale entre événements récents
            let recent = events.tail(10)
            let correlations = recent
                .pairs()
                .map((e1, e2) => attention_score(e1, e2))
                .filter(score => score > 0.9)
            
            # Pattern de dégradation: conso monte + pression baisse + haute corrélation
            power_trend > 0.05 and pressure_trend < -0.02 and correlations.len() > 5
    )
    .emit(
        alert_type: "hvac_degradation",
        severity: "high",
        unit_id: unit_id,
        message: "Dégradation détectée - maintenance recommandée",
        predicted_failure_days: estimate_ttf(events),
        timestamp: now()
    )
```

### 5. Confort multi-zones

```varpulis
# Fusion de tous les capteurs pour score de confort global
stream ComfortScore = merge(
    stream Temp from TemperatureReading,
    stream Humidity from HumidityReading
)
.partition_by(zone)
.window(10m)
.aggregate(
    zone: first(zone),
    avg_temp: avg(if type == "temperature" then value else null),
    avg_humidity: avg(if type == "humidity" then value else null),
    comfort_score: compute_pmv(avg_temp, avg_humidity)  # Predicted Mean Vote
)
.where(comfort_score < -1.0 or comfort_score > 1.0)  # Inconfort
.emit(
    alert_type: "comfort_issue",
    zone: zone,
    score: comfort_score,
    recommendation: if comfort_score < -1.0 then "increase_heating" else "increase_cooling"
)
```

## Configuration MVP

```varpulis
config:
    mode: "balanced"
    
    sources:
        - type: "simulator"
          name: "hvac_sim"
          events_per_second: 100
          zones: ["zone_a", "zone_b", "zone_c"]
    
    sinks:
        - type: "console"
          name: "alerts"
          filter: "alert_type != null"
        
        - type: "file"
          name: "metrics"
          path: "./output/metrics.jsonl"
    
    state:
        backend: "in_memory"
    
    observability:
        metrics:
            enabled: true
            endpoint: "0.0.0.0:9090"
```

## Données de simulation

Pour le MVP, on simule :
1. **Fonctionnement normal** : Températures stables dans les seuils
2. **Anomalie ponctuelle** : Pic de température zone B (salle serveurs)
3. **Dégradation progressive** : Consommation HVAC qui augmente sur 30min
4. **Corrélation inter-zones** : Problème zone A qui affecte zone B

## Métriques de succès MVP

- [ ] Parser VarpulisQL fonctionnel pour les exemples ci-dessus
- [ ] Exécution des streams de base (window, aggregate)
- [ ] Détection de l'anomalie de température
- [ ] Au moins un pattern avec attention_score simulé
- [ ] Émission des alertes vers console
