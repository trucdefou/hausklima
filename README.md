# hausklima

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-3.0-000000?logo=flask&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-003B57?logo=sqlite&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker_Compose-2496ED?logo=docker&logoColor=white)
![Zigbee2MQTT](https://img.shields.io/badge/Zigbee2MQTT-2.6-FFC135?logo=zigbee&logoColor=black)
![Mosquitto](https://img.shields.io/badge/Mosquitto-2.0-3C5280?logo=eclipsemosquitto&logoColor=white)
![Chart.js](https://img.shields.io/badge/Chart.js-FF6384?logo=chartdotjs&logoColor=white)
![CoreUI](https://img.shields.io/badge/CoreUI-321FDB?logo=coreui&logoColor=white)
![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)

Web dashboard that logs the temperature and humidity of every room in a house using Zigbee sensors.

The sensors publish their readings to Zigbee2MQTT. The dashboard receives them over MQTT, stores them in SQLite and plots them alongside the outdoor temperature from [Open-Meteo](https://open-meteo.com/).

## Features

- Shows the current temperature and humidity of each sensor, plus outdoor conditions (Open-Meteo, based on the configured location).
- Stores readings every N minutes (configurable from the dashboard) and charts them by date range, with zoom.
- Logs when windows are opened and closed, and changes to thermostats.
- Warns in the top bar when a sensor stops reporting, with signal status and an outage log in Settings.
- Shows house metrics: humidity loss and recovery, cooling, and energy per m² from the annual heating bill, for a house or an apartment.
- Supports manual sensors for rooms without a Zigbee sensor.
- Logs Xiaomi Mi Flora plant sensors over Bluetooth (soil moisture, light, fertility, soil temperature), with an ideal range per plant and a warning when it needs water or fertiliser.
- Tracks heating, hot water and cold water meter readings, with billing periods and consumption calculation. Hot water is rated against the Warmwasserspiegel; cold water is only counted.
- Exports temperatures, windows, thermostats and consumption to CSV.
- Available in Spanish, English and German; the language is picked from the browser.
- Password-protected.

## Screenshots

**Dashboard**: current conditions per room, thermostat levels and the last 24 hours.

![Dashboard](docs/screenshots/dashboard.png)

**Temperatures**: history by date range, with window openings and thermostat levels (dark theme).

![Temperatures](docs/screenshots/temperatures.png)

**Consumption**: heating meter consumption per period and per month.

![Consumption](docs/screenshots/consumption.png)

## Architecture

```
Zigbee sensors ──► USB dongle ──► zigbee2mqtt ──► mosquitto ──► panel ──► browser
                                     :8081          :1883        :8082
                                                                   │
                                                          data/climate.db (SQLite)
```

Plant sensors take a separate path: the `theengs` service reads them over the Bluetooth adapter of the host and publishes them to the same mosquitto broker.

| Service       | Image                        | Host port | Role                                      |
|---------------|------------------------------|-----------|-------------------------------------------|
| `mosquitto`   | `eclipse-mosquitto:2.0.20`   | 1883      | MQTT broker                               |
| `zigbee2mqtt` | `koenkk/zigbee2mqtt:2.6.2`   | 8081      | Zigbee → MQTT bridge, with web UI         |
| `theengs`     | `theengs/gateway`            | (host network) | Bluetooth LE to MQTT bridge for plant sensors |
| `panel`       | local build (`panel/`)       | 8082      | Flask + Waitress, API and static frontend |

The frontend is plain static files (CoreUI, Chart.js, chartjs-plugin-zoom, flatpickr) with no build step. The libraries are vendored in `panel/static/vendor`, so the dashboard loads nothing from a CDN.

## Requirements

- Linux with Docker and the Docker Compose v2 plugin. Tested on a Raspberry Pi running 64-bit Raspberry Pi OS.
- A USB Zigbee coordinator supported by Zigbee2MQTT. This project uses a **SONOFF Zigbee 3.0 USB Dongle Plus MG24** (`ember` adapter).
- Zigbee temperature and humidity sensors.
- Internet access from the host to query Open-Meteo (optional: without it the dashboard still works, but doesn't show the outdoor temperature).

## Installation

1. Clone the repo:

   ```bash
   git clone git@github.com:trucdefou/hausklima.git
   cd hausklima
   ```

2. Create `.env` from the example:

   ```bash
   cp .env.example .env
   chmod 600 .env
   ```

   | Variable         | Description                                                  |
   |------------------|--------------------------------------------------------------|
   | `PANEL_PASSWORD` | Password to log in to the dashboard                          |
   | `ZIGBEE_DONGLE`  | Stable path to the dongle: run `ls /dev/serial/by-id/` to find it |

3. Create the Zigbee2MQTT configuration at `zigbee2mqtt/data/configuration.yaml`. That folder isn't in the repo because it holds the Zigbee network key:

   ```yaml
   homeassistant:
     enabled: false
   mqtt:
     base_topic: zigbee2mqtt
     server: mqtt://mosquitto:1883
   serial:
     port: /dev/ttyACM0   # path inside the container; don't change it
     adapter: ember       # depends on your dongle
   frontend:
     enabled: true
     port: 8080
   availability:
     enabled: true
     passive:
       timeout: 120   # minutes without data before a battery sensor counts as offline
   advanced:
     log_level: info
     last_seen: ISO_8601_local   # real time of each reading
     network_key: GENERATE
     pan_id: GENERATE
     ext_pan_id: GENERATE
   ```

4. Create the data folder before the first start. If Docker creates it, it ends up owned by root and the dashboard (which runs as user 1000) can't write to it:

   ```bash
   mkdir -p data
   ```

5. Start the services:

   ```bash
   docker compose up -d --build
   ```

6. Pair the sensors from the Zigbee2MQTT UI at `http://<host>:8081` (*Permit join*).

7. Open the dashboard at `http://<host>:8082` and give each sensor a room name. The dashboard settings also let you set the location (for outdoor weather), the logging interval and the reference ranges for temperature and humidity.

## Plant sensors (Mi Flora)

The Xiaomi Mi Flora / Flower Care sensor (HHCCJCY01, sold "for the Mijia app") uses Bluetooth LE, not Zigbee. The `theengs` service ([Theengs Gateway](https://gateway.theengs.io/)) listens to its advertisements with the Bluetooth adapter built into the Raspberry Pi and publishes them to mosquitto under `home/TheengsGateway/BTtoMQTT/<MAC>`. It doesn't pair or connect to the sensor, so the app and the battery are not affected.

1. Make sure Bluetooth is on in the host:

   ```bash
   bluetoothctl show          # must show "Powered: yes"
   sudo rfkill unblock bluetooth
   sudo systemctl enable --now bluetooth
   ```

2. Place the sensor within about 10 m of the Pi (walls reduce the range a lot) and start the services with `docker compose up -d`.
3. Check that the data arrives (the sensor advertises about once a minute):

   ```bash
   docker run --rm --network host eclipse-mosquitto:2.0.20 mosquitto_sub -h 127.0.0.1 -t 'home/TheengsGateway/#' -v
   ```

   A Mi Flora shows up with `"model_id":"HHCCJCY01HHCC"` and the fields `moi` (soil moisture %), `lux`, `fer` (conductivity µS/cm) and `tempc`.
4. In the dashboard, the plant appears on the main page and in **Plants**. Name it and set its ideal ranges in **Settings > Sensors**.

If nothing arrives, set `PUBLISH_ALL=true` and `LOG_LEVEL=DEBUG` for `theengs` in `docker-compose.yml` to see what it receives. Battery level is not included in the passive advertisements, so it usually shows as empty.

## Data

Everything the dashboard generates lives in `data/`:

| File             | Contents                                                     |
|------------------|--------------------------------------------------------------|
| `climate.db`     | SQLite database with readings, windows, thermostats, meters and plant readings |
| `config.json`    | Location, room names, interval and ranges                    |
| `.session_key`   | Key used to sign session cookies (created automatically)     |

To back up, copy `data/` and `zigbee2mqtt/data/`. Without the latter you'd have to pair all the sensors again.

## Security

- Mosquitto accepts anonymous connections (`allow_anonymous true`). It's meant for a local network: don't expose port 1883 to the internet.
- The Zigbee2MQTT UI (8081) has no password.
- The dashboard uses plain HTTP without TLS. To access it from outside your home, use a VPN (WireGuard, Tailscale) or a reverse proxy with HTTPS.

## License

[GNU AGPL v3](LICENSE)
