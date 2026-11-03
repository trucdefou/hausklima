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

Panel web para registrar la temperatura y la humedad de cada habitación de una casa con sensores Zigbee.

Los sensores publican sus lecturas en Zigbee2MQTT. El panel las recibe por MQTT, las guarda en SQLite y las muestra en gráficos junto con la temperatura exterior de [Open-Meteo](https://open-meteo.com/).

## Qué hace

- Muestra la temperatura y la humedad actuales de cada sensor y las del exterior (Open-Meteo, según la ubicación configurada).
- Guarda las lecturas cada N minutos (configurable desde el panel) y las grafica por rango de fechas, con zoom.
- Registra cuándo se abren y se cierran las ventanas y los cambios en los termostatos.
- Acepta sensores manuales para habitaciones sin sensor Zigbee.
- Lleva lecturas de medidores de calefacción y agua caliente, con períodos de facturación y cálculo de consumo.
- Exporta temperaturas, ventanas, termostatos y consumo a CSV.
- Está en español, inglés y alemán; elige el idioma según el navegador.
- Pide contraseña para entrar.

## Arquitectura

```
Sensores Zigbee ──► Dongle USB ──► zigbee2mqtt ──► mosquitto ──► panel ──► navegador
                                     :8081          :1883         :8082
                                                                   │
                                                          datos/clima.db (SQLite)
```

| Servicio      | Imagen                       | Puerto host | Función                                   |
|---------------|------------------------------|-------------|-------------------------------------------|
| `mosquitto`   | `eclipse-mosquitto:2.0.20`   | 1883        | Broker MQTT                               |
| `zigbee2mqtt` | `koenkk/zigbee2mqtt:2.6.2`   | 8081        | Puente Zigbee → MQTT, con interfaz web    |
| `panel`       | build local (`panel/`)       | 8082        | Flask + Waitress, API y frontend estático |

El frontend usa archivos estáticos (CoreUI, Chart.js, chartjs-plugin-zoom, flatpickr), sin paso de build. Las librerías están copiadas en `panel/static/vendor`, así que el panel no carga nada de un CDN.

## Requisitos

- Linux con Docker y el plugin Docker Compose v2. Probado en una Raspberry Pi con Raspberry Pi OS de 64 bits.
- Un coordinador Zigbee USB compatible con Zigbee2MQTT. Este proyecto usa un **SONOFF Zigbee 3.0 USB Dongle Plus MG24** (adaptador `ember`).
- Sensores Zigbee de temperatura y humedad.
- Acceso a internet desde el host para consultar Open-Meteo (opcional: sin él, el panel funciona igual pero no muestra la temperatura exterior).

## Instalación

1. Clonar el repo:

   ```bash
   git clone git@github.com:trucdefou/hausklima.git
   cd hausklima
   ```

2. Crear el `.env` a partir del ejemplo:

   ```bash
   cp .env.example .env
   chmod 600 .env
   ```

   | Variable         | Descripción                                                   |
   |------------------|---------------------------------------------------------------|
   | `PANEL_PASSWORD` | Contraseña para entrar al panel                               |
   | `ZIGBEE_DONGLE`  | Ruta estable del dongle: `ls /dev/serial/by-id/` para verla   |

3. Crear la configuración de Zigbee2MQTT en `zigbee2mqtt/data/configuration.yaml`. Esa carpeta no está en el repo porque guarda la clave de la red Zigbee:

   ```yaml
   homeassistant:
     enabled: false
   mqtt:
     base_topic: zigbee2mqtt
     server: mqtt://mosquitto:1883
   serial:
     port: /dev/ttyACM0   # ruta dentro del contenedor; no cambiarla
     adapter: ember       # depende del dongle
   frontend:
     enabled: true
     port: 8080
   advanced:
     log_level: info
     network_key: GENERATE
     pan_id: GENERATE
     ext_pan_id: GENERATE
   ```

4. Crear la carpeta de datos antes del primer arranque. Si la crea Docker, queda como root y el panel (que corre como usuario 1000) no puede escribir en ella:

   ```bash
   mkdir -p datos
   ```

5. Levantar los servicios:

   ```bash
   docker compose up -d --build
   ```

6. Emparejar los sensores desde la interfaz de Zigbee2MQTT en `http://<host>:8081` (*Permit join*).

7. Entrar al panel en `http://<host>:8082` y asignar un nombre de habitación a cada sensor. En la configuración del panel también se fijan la ubicación (para el clima exterior), el intervalo de guardado y los rangos de referencia de temperatura y humedad.

## Datos

Todo lo que genera el panel queda en `datos/`:

| Archivo          | Contenido                                                       |
|------------------|-----------------------------------------------------------------|
| `clima.db`       | Base SQLite con lecturas, ventanas, termostatos y medidores     |
| `config.json`    | Ubicación, nombres de habitaciones, intervalo y rangos          |
| `.clave_sesion`  | Clave para firmar las cookies de sesión (se crea sola)          |

Para hacer una copia de seguridad hay que copiar `datos/` y `zigbee2mqtt/data/`. Sin esta última habría que volver a emparejar todos los sensores.

## Seguridad

- Mosquitto acepta conexiones anónimas (`allow_anonymous true`). Está pensado para una red local: no expongas el puerto 1883 a internet.
- La interfaz de Zigbee2MQTT (8081) no tiene contraseña.
- El panel usa HTTP sin TLS. Para acceder desde fuera de casa conviene usar una VPN (WireGuard, Tailscale) o un proxy inverso con HTTPS.

## Licencia

[GNU AGPL v3](LICENSE)
