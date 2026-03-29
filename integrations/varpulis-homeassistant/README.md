# Varpulis Home Assistant Integration

**Smart home pattern detection. Because "motion + door open + nobody home = alarm" shouldn't require YAML spaghetti.**

## Why Varpulis for Home Assistant?

Home Assistant automations follow a simple model: **trigger -> action**. That works for "turn on lights when motion detected." But what about:

- Door opens *while nobody is home*, then motion detected *within 2 minutes*? (Intrusion)
- You leave the house but the oven is *still drawing power 5 minutes later*? (Appliance left on)
- Humidity spikes *followed by* a water sensor trigger? (Leak before it floods)
- Power consumption stays abnormally high for *30 minutes straight*? (Appliance malfunction)
- Temperature drops 5 degrees in 15 minutes? (Window left open in winter)

These are **temporal sequences across multiple sensors** -- exactly what Varpulis was built for. Where HA automations check "is this condition true right now?", Varpulis tracks patterns unfolding over time and fires when the full sequence completes.

## Recipes

| Recipe | What It Detects | File |
|--------|----------------|------|
| **Intrusion Alert** | Door opens while away, then motion within 2 min | `recipes/security-intrusion.vpl` |
| **Appliance Left On** | Leave home with appliance still drawing power after 5 min | `recipes/appliance-left-on.vpl` |
| **Water Leak** | Humidity spike followed by water sensor trigger | `recipes/water-leak-sequence.vpl` |
| **Energy Anomaly** | Power consumption spike sustained 30+ minutes | `recipes/energy-anomaly.vpl` |
| **Comfort Swing** | Temperature drops 5+ degrees in 15 minutes | `recipes/comfort-swing.vpl` |

## How It Works

```
Home Assistant  --->  MQTT Broker  --->  Varpulis  --->  HA Webhooks/Notifications
   (sensors)         (state changes)    (pattern       (alerts, automations)
                                         detection)
```

1. HA publishes sensor state changes to MQTT (built-in integration)
2. Varpulis subscribes to MQTT topics matching HA entity patterns
3. VPL recipes detect temporal sequences across sensors
4. Matches trigger HA webhooks or notification services

## Quick Start

### 1. Enable MQTT in Home Assistant

Add to your HA `configuration.yaml`:

```yaml
mqtt:
  broker: localhost
  port: 1883
```

### 2. Run Varpulis alongside HA

Add to your `docker-compose.yml`:

```yaml
varpulis:
  image: ghcr.io/varpulis/varpulis:latest
  command: run --file /config/recipes/security-intrusion.vpl
  environment:
    - MQTT_HOST=homeassistant
    - MQTT_PORT=1883
  volumes:
    - ./recipes:/config/recipes
  depends_on:
    - homeassistant
```

### 3. Pick a recipe and customize

Edit entity IDs in the VPL file to match your setup:

```vpl
# Change these to your actual entity IDs
event DoorEvent:
    entity_id: str    # e.g., binary_sensor.front_door
    state: str        # "open" / "closed"
```

### 4. Test with sample events

```bash
varpulis simulate \
  -p recipes/security-intrusion.vpl \
  -e tests/security-intrusion.evt \
  -v -w 1
```

## Entity Naming Convention

Recipes use standard Home Assistant entity ID patterns:

| Entity Type | Example | MQTT Topic |
|------------|---------|------------|
| Door sensor | `binary_sensor.front_door` | `homeassistant/binary_sensor/front_door/state` |
| Motion sensor | `binary_sensor.hallway_motion` | `homeassistant/binary_sensor/hallway_motion/state` |
| Presence | `person.john` | `homeassistant/person/john/state` |
| Power meter | `sensor.washing_machine_power` | `homeassistant/sensor/washing_machine_power/state` |
| Temperature | `sensor.living_room_temperature` | `homeassistant/sensor/living_room_temperature/state` |
| Humidity | `sensor.bathroom_humidity` | `homeassistant/sensor/bathroom_humidity/state` |
| Water leak | `binary_sensor.kitchen_water_leak` | `homeassistant/binary_sensor/kitchen_water_leak/state` |

## Writing Your Own Recipes

VPL temporal patterns follow this structure:

```vpl
stream AlertName = EventA as a -> EventB as b -> EventC as c
    .within(5m)                              # time window
    .where(a.field == b.field)               # correlation
    .alert(webhook: "http://...", message: "...")  # notify HA
    .emit(field1: a.value, field2: b.value)  # output fields
```

The `->` operator means "followed by" -- EventA must happen before EventB, and EventB before EventC, all within the specified time window.

## Documentation

- [Varpulis Language Reference](https://varpulis.dev/docs/language)
- [SASE+ Pattern Syntax](https://varpulis.dev/docs/sase-patterns)
- [MQTT Connector Guide](https://varpulis.dev/docs/connectors/mqtt)
- [Sequence Patterns Tutorial](https://varpulis.dev/docs/tutorials/sequences)

## License

Same as Varpulis core. See [LICENSE](../../LICENSE).
