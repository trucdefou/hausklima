"""Temperature dashboard.

- Listens to Zigbee2MQTT sensors over MQTT.
- Stores readings (and the outdoor temperature) in SQLite (data/climate.db) every N minutes (configurable).
- Logs window openings in the same database.
- CSV files are generated when downloaded.
- Serves the dashboard web UI.
"""
import csv
import glob
import hashlib
import hmac
import io
import json
import os
import secrets
import sqlite3
import statistics
import threading
import time
import urllib.parse
import urllib.request
import uuid
import zipfile
from contextlib import contextmanager
from datetime import datetime, timedelta

import paho.mqtt.client as mqtt
from flask import Flask, Response, abort, jsonify, redirect, request, send_from_directory, session
from waitress import serve
from flask.sessions import SecureCookieSessionInterface
from werkzeug.exceptions import HTTPException

MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
BASE_TOPIC = os.environ.get("BASE_TOPIC", "zigbee2mqtt")
WEATHER_INTERVAL = int(os.environ.get("WEATHER_INTERVAL_SECONDS", "600"))
DATA_DIR = os.environ.get("DATA_DIR", "/data")
PORT = int(os.environ.get("PORT", "8000"))

CONFIG_PATH = os.path.join(DATA_DIR, "config.json")
LEGACY_WINDOWS_CSV_PATH = os.path.join(DATA_DIR, "ventanas.csv")  # old format, only for migration
DB_PATH = os.path.join(DATA_DIR, "climate.db")
TEMP_COLS = ["timestamp", "sensor", "temperature", "humidity", "battery", "linkquality", "last_reading"]
WINDOW_COLS = ["id", "sensor", "start_at", "end_at", "note"]
OUTDOOR = "outdoor"
# An older reading is not stored: the sensor is not transmitting. Same as availability.passive.timeout in Zigbee2MQTT.
VALID_READING_MINUTES = 120
DEFAULT_CONFIG = {
    "place": {"name": "Wiesbaden, Hessen, Deutschland", "lat": 50.0826, "lon": 8.24,
                  "names": {"es": "Wiesbaden, Hessen, Alemania", "en": "Wiesbaden, Hesse, Germany",
                              "de": "Wiesbaden, Hessen, Deutschland"}},
    "rooms": {},
    "interval_minutes": 5,
    "live_seconds": 10,    # how often the browser checks for new data (does not affect what is stored)
    "manual_sensors": [],   # ids of sensors whose records are entered by hand (e.g. "manual_1a2b3c4d")
    # recommended ranges, drawn as a reference on the chart of each metric
    "humidity_ref": {"min": 40, "max": 60},
    "temp_ref": {"min": 18, "max": 22},
    "move_in_date": "",    # YYYY-MM-DD: consumption before this date belonged to the previous tenant
    # m2_meters: floor area per heating meter; m2_building is only used if is_apartment
    "home": {"is_apartment": False, "m2_total": None, "m2_building": None, "m2_meters": {}, "people": None, "energy": None},
    # Warmwasserspiegel (co2online) per year: limit in m³/year for categories A, B and C, from 1 to 5 people. Above that, D.
    "water_benchmarks": {"2024": {
        "house": [[10, 13, 18], [17, 26, 36], [29.7, 40, 53], [40, 56, 85], [50, 70, 100]],
        "apartment": [[7.3, 11, 17], [15, 23, 33.6], [21.6, 32.7, 47.2], [26, 40, 59], [32.9, 50, 74.5]],
    }},
    # Heizspiegel (co2online) per billing year: limits A, B and C in kWh and € per m² and year, by building floor-area band
    "heizspiegels": {
        "2023": {"edition": 2024, "min_m2": 100, "bands": [
            {"until": 250, "rows": {
                "Erdgas": {"kwh": [78, 143, 230], "cost": [13.1, 21.3, 31.9]},
                "Heizöl": {"kwh": [95, 148, 230], "cost": [13.0, 18.3, 26.1]},
                "Fernwärme": {"kwh": [66, 123, 197], "cost": [10.3, 16.7, 24.5]},
                "Wärmepumpe": {"kwh": [19, 39, 101], "cost": [8.0, 15.2, 29.9]},
                "Holzpellets": {"kwh": [71, 130, 219], "cost": [8.9, 13.3, 19.6]},
            }},
            {"until": 500, "rows": {
                "Erdgas": {"kwh": [76, 137, 218], "cost": [12.5, 20.1, 29.6]},
                "Heizöl": {"kwh": [89, 141, 217], "cost": [12.1, 17.2, 24.3]},
                "Fernwärme": {"kwh": [63, 117, 191], "cost": [9.9, 16.2, 23.9]},
                "Wärmepumpe": {"kwh": [18, 37, 97], "cost": [7.7, 14.0, 29.0]},
                "Holzpellets": {"kwh": [67, 119, 205], "cost": [8.2, 12.0, 18.0]},
            }},
            {"until": 1000, "rows": {
                "Erdgas": {"kwh": [73, 131, 205], "cost": [11.9, 19.0, 27.6]},
                "Heizöl": {"kwh": [84, 135, 205], "cost": [11.2, 16.3, 22.7]},
                "Fernwärme": {"kwh": [60, 113, 186], "cost": [9.6, 15.7, 23.5]},
                "Wärmepumpe": {"kwh": [18, 36, 93], "cost": [7.5, 13.0, 28.2]},
            }},
            {"until": None, "rows": {
                "Erdgas": {"kwh": [72, 128, 197], "cost": [11.6, 18.4, 26.4]},
                "Heizöl": {"kwh": [80, 132, 197], "cost": [10.7, 15.7, 21.7]},
                "Fernwärme": {"kwh": [58, 110, 183], "cost": [9.4, 15.4, 23.2]},
                "Wärmepumpe": {"kwh": [17, 35, 90], "cost": [7.3, 12.4, 27.7]},
            }},
        ]},
        "2024": {"edition": 2025, "min_m2": 80, "bands": [
            {"until": 150, "rows": {
                "Erdgas": {"kwh": [62, 121, 207], "cost": [6.9, 13.6, 23.2]},
                "Heizöl": {"kwh": [100, 165, 263], "cost": [9.9, 16.4, 26.1]},
                "Fernwärme": {"kwh": [38, 89, 191], "cost": [6.4, 15.2, 32.5]},
                "Wärmepumpe": {"kwh": [20, 36, 82], "cost": [5.1, 9.2, 21.1]},
                "Holzpellets": {"kwh": [74, 148, 249], "cost": [4.4, 8.8, 14.8]},
            }},
            {"until": 250, "rows": {
                "Erdgas": {"kwh": [64, 116, 187], "cost": [7.2, 13.0, 21.0]},
                "Heizöl": {"kwh": [92, 142, 220], "cost": [9.1, 14.1, 21.8]},
                "Fernwärme": {"kwh": [41, 94, 169], "cost": [7.0, 16.1, 28.7]},
                "Wärmepumpe": {"kwh": [18, 33, 77], "cost": [4.6, 8.5, 19.8]},
                "Holzpellets": {"kwh": [72, 126, 219], "cost": [4.3, 7.5, 13.0]},
            }},
            {"until": 500, "rows": {
                "Erdgas": {"kwh": [61, 114, 185], "cost": [9.0, 14.7, 22.3]},
                "Heizöl": {"kwh": [78, 123, 197], "cost": [10.3, 14.7, 22.0]},
                "Fernwärme": {"kwh": [36, 112, 203], "cost": [7.8, 17.5, 29.5]},
                "Wärmepumpe": {"kwh": [17, 30, 69], "cost": [6.3, 9.7, 19.7]},
                "Holzpellets": {"kwh": [60, 113, 196], "cost": [5.8, 8.8, 13.3]},
            }},
            {"until": None, "rows": {
                "Erdgas": {"kwh": [59, 115, 177], "cost": [8.7, 14.8, 21.5]},
                "Heizöl": {"kwh": [68, 127, 198], "cost": [9.3, 15.1, 22.2]},
                "Fernwärme": {"kwh": [46, 87, 144], "cost": [9.7, 16.3, 25.7]},
            }},
        ]},
    },
}
LIVE_SECONDS_RANGE = (3, 300)
HEIZ_SOURCES = ("Erdgas", "Heizöl", "Fernwärme", "Wärmepumpe", "Holzpellets")
REF_LIMITS = {"humidity_ref": (0, 100, "err.humidity_ref"), "temp_ref": (-10, 40, "err.temp_ref")}

lock = threading.RLock()
latest = {}           # friendly_name -> latest reading
ieee_by_name = {}   # friendly_name -> ieee_address
sensor_connection = {}   # ieee_address -> "online" / "offline" (Zigbee2MQTT availability)
pending_connection = {}  # friendly_name -> state received before its IEEE address is known
live_version = 0         # increases with every new datum; the browser polls it to know when to refresh
outdoor = {}
refresh_weather = threading.Event()
change_interval = threading.Event()

PASSWORD = os.environ.get("PANEL_PASSWORD", "")
if not PASSWORD:
    raise SystemExit("Missing PANEL_PASSWORD variable (set it in the .env file)")

app = Flask(__name__, static_folder="static")


def secret_key():
    """Key used to sign session cookies; generated once and stored in /data."""
    path = os.path.join(DATA_DIR, ".session_key")
    legacy_path = os.path.join(DATA_DIR, ".clave_sesion")
    if os.path.exists(legacy_path) and not os.path.exists(path):
        os.replace(legacy_path, path)
    try:
        with open(path, "rb") as f:
            return f.read()
    except FileNotFoundError:
        os.makedirs(DATA_DIR, exist_ok=True)
        key = secrets.token_bytes(32)
        with open(path, "wb") as f:
            f.write(key)
        os.chmod(path, 0o600)
        return key


class ProtocolAwareSession(SecureCookieSessionInterface):
    """'Secure' cookie when accessed over HTTPS (Cloudflare tunnel); no Secure flag on the local HTTP network."""

    def get_cookie_secure(self, app):
        return request.is_secure


app.session_interface = ProtocolAwareSession()
app.secret_key = secret_key()
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=timedelta(days=180),
)
# If the password changes, open sessions stop being valid.
FINGERPRINT = hashlib.sha256(app.secret_key + PASSWORD.encode()).hexdigest()[:16]


# ---------- utilities ----------

def now():
    return datetime.now().isoformat(timespec="seconds")


def normalize_date(value):
    if not value:
        return ""
    try:
        return datetime.fromisoformat(str(value)).replace(tzinfo=None, microsecond=0).isoformat(timespec="seconds")
    except ValueError:
        abort(400, "err.invalid_date")


def write_atomic(path, content):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        f.write(content)
    os.replace(tmp, path)


def fetch_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "panel-temperatures/1.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.load(r)


def read_config():
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            cfg = json.load(f)
    except (OSError, ValueError):
        cfg = {}
    if "water_benchmark" in cfg:   # previous format: a single table, the 2024 one
        cfg.setdefault("water_benchmarks", {"2024": cfg.pop("water_benchmark")})
    for key, value in DEFAULT_CONFIG.items():
        cfg.setdefault(key, json.loads(json.dumps(value)))
    return cfg


OUTDOOR_NAME = {"es": "Afuera", "en": "Outdoors", "de": "Außen"}
LANGS = ("es", "en", "de")


def name_of(sensor, cfg, lang="es"):
    if sensor == OUTDOOR:
        return OUTDOOR_NAME.get(lang, "Outdoor")
    return cfg["rooms"].get(sensor) or f"Sensor {sensor[-4:]}"


def requested_lang():
    """Request language: ?lang=, 'lang' cookie or Accept-Language header."""
    for value in (request.args.get("lang"), request.cookies.get("lang")):
        if value in LANGS:
            return value
    return request.accept_languages.best_match(LANGS) or "es"


# ---------- MQTT ----------

def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected to MQTT ({reason_code})", flush=True)
    client.subscribe(f"{BASE_TOPIC}/+")
    client.subscribe(f"{BASE_TOPIC}/+/availability")
    client.subscribe(f"{BASE_TOPIC}/bridge/devices")


def reading_time(data):
    """Time at which Zigbee2MQTT received the reading (last_seen). Without it, retained and
    republished messages would look freshly arrived even if the sensor has not transmitted for hours."""
    try:
        return datetime.fromisoformat(data["last_seen"]).replace(tzinfo=None, microsecond=0).isoformat(timespec="seconds")
    except (KeyError, TypeError, ValueError):
        return now()


def mark_changed():
    global live_version
    with lock:
        live_version += 1


def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload)
    except ValueError:
        return
    if msg.topic.endswith("/availability"):
        name = msg.topic[len(BASE_TOPIC) + 1:-len("/availability")]
        if name != "bridge" and isinstance(data, dict) and data.get("state") in ("online", "offline"):
            update_connection_state(name, data["state"])
        return
    if msg.topic == f"{BASE_TOPIC}/bridge/devices":
        if not isinstance(data, list):
            return
        with lock:
            ieee_by_name.clear()
            for d in data:
                if d.get("type") != "Coordinator":
                    ieee_by_name[d["friendly_name"]] = d["ieee_address"]
            for name in list(latest):
                if name not in ieee_by_name:
                    del latest[name]
            pending = dict(pending_connection)
            pending_connection.clear()
        for name, state in pending.items():
            update_connection_state(name, state)
        close_outages_of_removed()
        return
    name = msg.topic[len(BASE_TOPIC) + 1:]
    if name == "bridge" or not isinstance(data, dict) or "temperature" not in data:
        return
    with lock:
        if ieee_by_name and name not in ieee_by_name:
            return  # old message from a renamed/removed device
        latest[name] = {
            "temperature": data.get("temperature"),
            "humidity": data.get("humidity"),
            "battery": data.get("battery"),
            "linkquality": data.get("linkquality"),
            "last_reading": reading_time(data),
        }
    mark_changed()


def current_readings():
    """Latest readings keyed by IEEE address (stable even if renamed in Zigbee2MQTT)."""
    with lock:
        return {ieee_by_name.get(n, n): dict(v) for n, v in latest.items()}


def update_connection_state(name, state):
    """Opens an outage when the sensor goes offline and closes it when it comes back online.

    It is idempotent: the retained state received when the panel restarts does not duplicate outages."""
    with lock:
        ieee = ieee_by_name.get(name)
        if ieee is None:
            pending_connection[name] = state
            return
        sensor_connection[ieee] = state
        reading = latest.get(name, {})
    mark_changed()
    try:
        with connection() as conn:
            is_open = conn.execute("SELECT 1 FROM sensor_outages WHERE sensor = ? AND until IS NULL", (ieee,)).fetchone()
            if state == "offline" and is_open is None:
                previous = conn.execute("SELECT last_reading, linkquality FROM readings WHERE sensor = ? "
                                     "AND last_reading IS NOT NULL ORDER BY timestamp DESC LIMIT 1", (ieee,)).fetchone()
                last = reading.get("last_reading") or (previous and previous["last_reading"])
                conn.execute("INSERT INTO sensor_outages (id, sensor, since, last_reading, linkquality) VALUES (?, ?, ?, ?, ?)",
                            (uuid.uuid4().hex[:12], ieee, now(), last,
                             reading.get("linkquality") or (previous and previous["linkquality"])))
                # Until the outage is detected the last value kept being stored with a new time: those copies are not measurements.
                # The first row of that reading is the real datum and is kept.
                conn.execute("DELETE FROM readings WHERE sensor = ? AND last_reading = ? AND timestamp > "
                            "(SELECT MIN(timestamp) FROM readings WHERE sensor = ? AND last_reading = ?)",
                            (ieee, last, ieee, last))
            elif state == "online" and is_open is not None:
                conn.execute("UPDATE sensor_outages SET until = ? WHERE sensor = ? AND until IS NULL", (now(), ieee))
    except sqlite3.Error as e:  # a database error must not kill the MQTT thread
        print(f"Error logging the outage of {name}: {e}", flush=True)


def close_outages_of_removed():
    """A device removed from Zigbee2MQTT will never send 'online' again: its alert must not stay forever."""
    with lock:
        known = list(ieee_by_name.values())
    if not known:
        return
    try:
        with connection() as conn:
            conn.execute(f"UPDATE sensor_outages SET until = ? WHERE until IS NULL AND sensor NOT IN "
                        f"({', '.join('?' * len(known))})", [now(), *known])
    except sqlite3.Error as e:
        print(f"Error closing outages: {e}", flush=True)


# ---------- database (SQLite) ----------

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    timestamp     TEXT NOT NULL,
    sensor         TEXT NOT NULL,
    temperature    REAL,
    humidity        REAL,
    battery        INTEGER,
    linkquality    INTEGER,
    last_reading TEXT,
    PRIMARY KEY (timestamp, sensor)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS readings_by_sensor ON readings (sensor, timestamp);

CREATE TABLE IF NOT EXISTS windows (
    id     TEXT PRIMARY KEY,
    sensor TEXT NOT NULL,
    start_at TEXT NOT NULL,
    end_at    TEXT,               -- NULL while the window is still open
    note   TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS windows_by_start ON windows (start_at);

-- Thermostat level of each room: every row is a change and holds until the next one
CREATE TABLE IF NOT EXISTS thermostats (
    id         TEXT PRIMARY KEY,
    sensor     TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    level      REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS thermostats_by_sensor ON thermostats (sensor, timestamp);

-- Heating meters (e.g. the Heizkostenverteiler of each radiator or the gas meter)
CREATE TABLE IF NOT EXISTS meters (
    id     TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    sensor TEXT NOT NULL DEFAULT '',          -- associated room (optional)
    unit TEXT NOT NULL DEFAULT ''
);
-- Counter readings; 'reset' = the counter went back to 0 before this reading (period change)
CREATE TABLE IF NOT EXISTS meter_readings (
    id         TEXT PRIMARY KEY,
    meter    TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    value      REAL NOT NULL,
    reset   INTEGER NOT NULL DEFAULT 0,
    note       TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS meter_readings_by_date ON meter_readings (meter, timestamp);

-- Billing periods (Abrechnungszeiträume), shared by the whole house. Dates YYYY-MM-DD, 'end_at' inclusive.
-- At the start of each period the counters go back to 0.
CREATE TABLE IF NOT EXISTS periods (
    id     TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    start_at TEXT NOT NULL,
    end_at    TEXT NOT NULL
);
-- Final value of each meter in each period (the Vorjahres-/Stichtagswert shown by the device)
CREATE TABLE IF NOT EXISTS period_closings (
    meter TEXT NOT NULL,
    period TEXT NOT NULL,
    value   REAL NOT NULL,
    PRIMARY KEY (meter, period)
);

-- Annual heating bill data, per period. Building figures only apply to an apartment.
CREATE TABLE IF NOT EXISTS period_bills (
    period           TEXT PRIMARY KEY,
    kwh_home      REAL,
    units_home REAL,
    kwh_building      REAL,
    units_building REAL,
    price_kwh        REAL,            -- energy price that year, per kWh
    paid            REAL             -- total amount paid at period close, in €
);

-- Sensor connection outages (Zigbee2MQTT availability).
-- 'since' is when it was detected (delayed by the availability timeout); 'last_reading' is the last real datum.
CREATE TABLE IF NOT EXISTS sensor_outages (
    id             TEXT PRIMARY KEY,
    sensor         TEXT NOT NULL,
    since          TEXT NOT NULL,
    until          TEXT,           -- NULL while the sensor remains disconnected
    last_reading TEXT,
    linkquality    INTEGER
);
CREATE INDEX IF NOT EXISTS outages_by_since ON sensor_outages (since);

-- Daily mean outdoor temperature (Open-Meteo): splits consumption between readings according to the cold.
-- Can be downloaded again; depends on the configured place and is deleted if it changes.
CREATE TABLE IF NOT EXISTS daily_weather (
    date      TEXT PRIMARY KEY,   -- YYYY-MM-DD
    mean_temp REAL NOT NULL
);
"""
LEVEL_MAX = 5
# 0.5 = ❄ frost protection (between 0 and 1 on the dial); from 1 to 5 half points are also valid
VALID_LEVELS = {0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5}


@contextmanager
def connection():
    """Short-lived connection per operation: commits on success and rolls back on error."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def to_number(value, kind=float):
    try:
        return kind(value) if value not in ("", None) else None
    except (TypeError, ValueError):
        return None


def migrate_csv():
    """Imports CSV files in the old format and renames them to *.migrado (nothing is deleted)."""
    files = sorted(glob.glob(os.path.join(DATA_DIR, "temperaturas_*.csv")))
    has_windows = os.path.exists(LEGACY_WINDOWS_CSV_PATH)
    if not files and not has_windows:
        return
    total_l = total_v = 0
    with connection() as conn:
        for path in files:
            with open(path, newline="", encoding="utf-8") as f:
                rows = [(r["fecha_hora"], r["sensor"], to_number(r.get("temperatura")), to_number(r.get("humedad")),
                          to_number(r.get("bateria"), int), to_number(r.get("linkquality"), int), r.get("ultima_lectura") or None)
                         for r in csv.DictReader(f) if r.get("fecha_hora", "")[:1].isdigit()]  # skips repeated headers
            conn.executemany("INSERT OR IGNORE INTO readings VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
            total_l += len(rows)
        if has_windows:
            with open(LEGACY_WINDOWS_CSV_PATH, newline="", encoding="utf-8") as f:
                rows = [(r["id"], r["sensor"], r["inicio"], r.get("fin") or None, r.get("nota") or "")
                         for r in csv.DictReader(f)]
            conn.executemany("INSERT OR IGNORE INTO windows VALUES (?, ?, ?, ?, ?)", rows)
            total_v = len(rows)
    for path in files + ([LEGACY_WINDOWS_CSV_PATH] if has_windows else []):
        os.replace(path, path + ".migrado")
    print(f"Migration: {total_l} readings and {total_v} windows imported into {DB_PATH}", flush=True)


# Columns from a previous version (period start per meter and closing per reading), replaced by
# the 'periods' and 'period_closings' tables. Kept so that existing databases remain valid.
ADDED_COLUMNS = [
    ("meters", "period_start", "TEXT NOT NULL DEFAULT ''"),
    ("meter_readings", "closing_value", "REAL"),
    # 'heating' (Heizkostenverteiler: goes back to 0 each period), 'hot_water' (Warmwasserzähler) or
    # 'cold_water' (Kaltwasserzähler). Water counters always add up; only hot water is rated with the Warmwasserspiegel.
    ("meters", "kind", "TEXT NOT NULL DEFAULT 'heating'"),
    ("period_bills", "price_kwh", "REAL"),
    ("period_bills", "paid", "REAL"),
]
WATER_KINDS = ("hot_water", "cold_water")
METER_KINDS = ("heating",) + WATER_KINDS


# ---------- migration from the original Spanish names ----------

# Names used by earlier versions on disk. They only appear here, to convert existing installations.
LEGACY_DB_PATH = os.path.join(DATA_DIR, "clima.db")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")
LEGACY_TABLES = {
    "lecturas": ("readings", {"fecha_hora": "timestamp", "temperatura": "temperature", "humedad": "humidity",
                              "bateria": "battery", "ultima_lectura": "last_reading"}),
    "ventanas": ("windows", {"inicio": "start_at", "fin": "end_at", "nota": "note"}),
    "termostatos": ("thermostats", {"fecha_hora": "timestamp", "nivel": "level"}),
    "medidores": ("meters", {"nombre": "name", "unidad": "unit", "inicio_periodo": "period_start", "tipo": "kind"}),
    "lecturas_medidor": ("meter_readings", {"medidor": "meter", "fecha_hora": "timestamp", "valor": "value", "reinicio": "reset",
                                            "nota": "note", "valor_cierre": "closing_value"}),
    "periodos": ("periods", {"nombre": "name", "inicio": "start_at", "fin": "end_at"}),
    "cierres_periodo": ("period_closings", {"medidor": "meter", "periodo": "period", "valor": "value"}),
    "facturas_periodo": ("period_bills", {"periodo": "period", "kwh_vivienda": "kwh_home", "unidades_vivienda": "units_home",
                                          "kwh_edificio": "kwh_building", "unidades_edificio": "units_building",
                                          "precio_kwh": "price_kwh", "pagado": "paid"}),
    "cortes_sensor": ("sensor_outages", {"desde": "since", "hasta": "until", "ultima_lectura": "last_reading"}),
    "clima_diario": ("daily_weather", {"fecha": "date", "temp_media": "mean_temp"}),
}
LEGACY_INDEXES = ("lecturas_por_sensor", "ventanas_por_inicio", "termostatos_por_sensor", "lecturas_medidor_por_fecha", "cortes_por_desde")
LEGACY_CONFIG_KEYS = {
    "ubicacion": "place", "nombre": "name", "nombres": "names", "habitaciones": "rooms", "intervalo_minutos": "interval_minutes",
    "segundos_vivo": "live_seconds", "sensores_manuales": "manual_sensors", "humedad_ref": "humidity_ref",
    "fecha_mudanza": "move_in_date", "vivienda": "home", "es_departamento": "is_apartment", "m2_edificio": "m2_building",
    "m2_medidores": "m2_meters", "personas": "people", "energia": "energy", "espejos_agua": "water_benchmarks",
    "espejo_agua": "water_benchmark", "casa": "house", "departamento": "apartment", "edicion": "edition",
    "franjas": "bands", "hasta": "until", "filas": "rows", "costo": "cost",
}


def rename_legacy_keys(value):
    if isinstance(value, dict):
        return {LEGACY_CONFIG_KEYS.get(k, k): rename_legacy_keys(v) for k, v in value.items()}
    if isinstance(value, list):
        return [rename_legacy_keys(v) for v in value]
    return value


def migrate_legacy_db(backup_suffix):
    """Backs up the old database, renames its tables, columns and stored values, then moves it to DB_PATH."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    source = sqlite3.connect(LEGACY_DB_PATH)
    backup = sqlite3.connect(os.path.join(BACKUP_DIR, f"clima-{backup_suffix}.db"))
    with backup:
        source.backup(backup)
    backup.close()
    source.close()

    conn = sqlite3.connect(LEGACY_DB_PATH, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        for index in LEGACY_INDEXES:
            conn.execute(f"DROP INDEX IF EXISTS {index}")
        for old_table, (new_table, columns) in LEGACY_TABLES.items():
            if old_table not in tables:
                continue
            conn.execute(f"ALTER TABLE {old_table} RENAME TO {new_table}")
            existing = {r[1] for r in conn.execute(f"PRAGMA table_info({new_table})")}
            for old_column, new_column in columns.items():
                if old_column in existing:
                    conn.execute(f"ALTER TABLE {new_table} RENAME COLUMN {old_column} TO {new_column}")
        conn.execute("UPDATE readings SET sensor = ? WHERE sensor = 'afuera'", (OUTDOOR,))
        conn.execute("UPDATE meters SET kind = 'heating' WHERE kind = 'calefaccion'")
        conn.execute("UPDATE meters SET kind = 'hot_water' WHERE kind = 'agua_caliente'")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    conn.execute("PRAGMA journal_mode=DELETE")  # no -wal/-shm files left behind when moving the database
    conn.close()
    os.replace(LEGACY_DB_PATH, DB_PATH)
    print(f"Database migrated to {DB_PATH}", flush=True)


def migrate_legacy_config(backup_suffix):
    with open(CONFIG_PATH, encoding="utf-8") as f:
        raw = f.read()
    os.makedirs(BACKUP_DIR, exist_ok=True)
    write_atomic(os.path.join(BACKUP_DIR, f"config-{backup_suffix}.json"), raw)
    write_atomic(CONFIG_PATH, json.dumps(rename_legacy_keys(json.loads(raw)), ensure_ascii=False, indent=2))
    print("Configuration migrated to the English keys", flush=True)


def migrate_legacy_storage():
    """One-time conversion of data written by versions that used Spanish names. Backups go to BACKUP_DIR."""
    suffix = "pre-english-" + datetime.now().strftime("%Y%m%d-%H%M%S")
    if os.path.exists(LEGACY_DB_PATH) and not os.path.exists(DB_PATH):
        migrate_legacy_db(suffix)
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            legacy_config = "habitaciones" in json.load(f)
    except (OSError, ValueError):
        legacy_config = False
    if legacy_config:
        migrate_legacy_config(suffix)


def init_db():
    with connection() as conn:
        conn.execute("PRAGMA journal_mode=WAL")  # simultaneous reads and writes without blocking
        conn.executescript(SCHEMA)
        for table, column, kind in ADDED_COLUMNS:
            if column not in {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {kind}")
    migrate_csv()


# ---------- periodic saving ----------

def save_readings(date=None):
    date = date or datetime.now()
    limit = (date - timedelta(minutes=VALID_READING_MINUTES)).isoformat(timespec="seconds")
    with lock:
        a = dict(outdoor)
        states = dict(sensor_connection)
    # A sensor without recent data is left as a gap, not as a flat line with its last value
    rows = [{"timestamp": date.isoformat(timespec="seconds"), "sensor": s, **v}
             for s, v in sorted(current_readings().items())
             if v["last_reading"] >= limit and states.get(s) != "offline"]
    if a.get("temperature") is not None and \
            datetime.fromisoformat(a["time"]) > date - timedelta(seconds=3 * WEATHER_INTERVAL):
        rows.append({"timestamp": date.isoformat(timespec="seconds"), "sensor": OUTDOOR,
                      "temperature": a["temperature"], "humidity": a["humidity"], "battery": None,
                      "linkquality": None, "last_reading": a["time"]})
    if not rows:
        print("No readings yet, nothing to store", flush=True)
        return 0
    with connection() as conn:
        conn.executemany(f"INSERT OR REPLACE INTO readings ({', '.join(TEMP_COLS)}) "
                        f"VALUES ({', '.join(':' + c for c in TEMP_COLS)})", rows)
    print(f"{len(rows)} readings stored", flush=True)
    return len(rows)


def read_instantly(iso_date):
    """Stores a reading of every sensor at the exact time a window was opened/closed."""
    try:
        save_readings(datetime.fromisoformat(iso_date))
    except Exception as e:  # the window has already been logged
        print(f"Error storing the window reading: {e}", flush=True)


def save_loop():
    while True:
        interval = int(read_config()["interval_minutes"]) * 60
        wait = interval - time.time() % interval  # aligns to 18:00, 18:05, ...
        if change_interval.wait(wait):
            change_interval.clear()
            continue
        try:
            save_readings()
        except Exception as e:  # the loop must not die
            print(f"Error storing: {e}", flush=True)


# ---------- outdoor weather ----------

GEOCODING = "https://geocoding-api.open-meteo.com/v1/"
WEATHER_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive?"
ARCHIVE_INTERVAL_HOURS = 6
next_archive_fetch = 0.0   # time.time() from which the archive is queried again


def place_text(r):
    return ", ".join(x for x in (r.get("name"), r.get("admin1"), r.get("country")) if x)


def place_names(u, id_geo=None):
    """Name of the place in each language according to Open-Meteo, or None if the query failed.

    With the Open-Meteo id it is requested directly; without it (old configurations) it searches by name
    and takes the result matching the stored coordinates."""
    names = {}
    try:
        for lang in LANGS:
            if id_geo:
                r = fetch_json(GEOCODING + "get?" + urllib.parse.urlencode({"id": int(id_geo), "language": lang}))
            else:
                search = urllib.parse.urlencode({"name": u["name"].split(",")[0].strip(), "count": 10,
                                                   "language": lang, "format": "json"})
                r = next((x for x in fetch_json(GEOCODING + "search?" + search).get("results") or []
                          if abs(x["latitude"] - u["lat"]) < 0.1 and abs(x["longitude"] - u["lon"]) < 0.1), None)
            if r and r.get("name"):
                names[lang] = place_text(r)[:100]
    except Exception as e:  # noqa: BLE001
        print(f"Could not translate the place names: {e}", flush=True)
        return None
    return names


def fill_place_names():
    """Adds per-language names to a place saved before they existed."""
    u = read_config()["place"]
    if "names" in u:
        return
    names = place_names(u)
    if names is None:
        return  # no connection: retried on the next pass
    with lock:
        cfg = read_config()
        if cfg["place"].get("lat") == u["lat"] and cfg["place"].get("lon") == u["lon"]:
            cfg["place"]["names"] = names
            write_atomic(CONFIG_PATH, json.dumps(cfg, ensure_ascii=False, indent=2))

def update_weather():
    u = read_config()["place"]
    url = "https://api.open-meteo.com/v1/forecast?" + urllib.parse.urlencode({
        "latitude": u["lat"], "longitude": u["lon"], "timezone": "auto",
        "current": "temperature_2m,relative_humidity_2m,apparent_temperature,weather_code,wind_speed_10m",
    })
    c = fetch_json(url)["current"]
    with lock:
        outdoor.clear()
        outdoor.update({
            "temperature": c["temperature_2m"], "humidity": c["relative_humidity_2m"],
            "feels_like": c["apparent_temperature"], "code": c["weather_code"],
            "wind": c["wind_speed_10m"], "place": u["name"], "time": now(), "error": None,
        })


def fill_daily_weather():
    """Downloads the missing daily temperatures from the first period or meter reading until yesterday."""
    global next_archive_fetch
    if time.time() < next_archive_fetch:
        return
    with connection() as conn:
        start_at = conn.execute("SELECT MIN(f) AS f FROM (SELECT MIN(start_at) AS f FROM periods "
                             "UNION ALL SELECT MIN(substr(timestamp, 1, 10)) FROM meter_readings)").fetchone()["f"]
        known = {r["date"] for r in conn.execute("SELECT date FROM daily_weather")}
    if not start_at:
        return
    first = datetime.fromisoformat(start_at).date()
    days = [first + timedelta(days=i) for i in range((datetime.now().date() - first).days)]
    missing = [d.isoformat() for d in days if d.isoformat() not in known]
    if missing:
        u = read_config()["place"]
        d = fetch_json(WEATHER_ARCHIVE_URL + urllib.parse.urlencode({
            "latitude": u["lat"], "longitude": u["lon"], "timezone": "auto", "daily": "temperature_2m_mean",
            "start_date": missing[0], "end_date": missing[-1]}))["daily"]
        rows = [(f, t) for f, t in zip(d["time"], d["temperature_2m_mean"]) if t is not None]
        with connection() as conn:
            conn.executemany("INSERT OR REPLACE INTO daily_weather (date, mean_temp) VALUES (?, ?)", rows)
        print(f"Weather history: {len(rows)} days stored", flush=True)
    next_archive_fetch = time.time() + ARCHIVE_INTERVAL_HOURS * 3600


def weather_loop():
    while True:
        try:
            fill_place_names()
        except Exception as e:  # noqa: BLE001
            print(f"Error filling in the place: {e}", flush=True)
        try:
            fill_daily_weather()
        except Exception as e:  # without history, consumption is split evenly
            print(f"Error fetching the weather history: {e}", flush=True)
        try:
            update_weather()
        except Exception as e:  # noqa: BLE001
            print(f"Error fetching the weather: {e}", flush=True)
            with lock:
                outdoor["error"] = str(e)
        refresh_weather.wait(WEATHER_INTERVAL)
        refresh_weather.clear()


# ---------- temperatures ----------

def read_temperatures(since, until, sensors):
    cond, params = [], []
    if since:
        cond.append("timestamp >= ?")
        params.append(since)
    if until:
        cond.append("timestamp <= ?")
        params.append(until)
    if sensors:
        cond.append(f"sensor IN ({', '.join('?' * len(sensors))})")
        params.extend(sorted(sensors))
    sql = f"SELECT {', '.join(TEMP_COLS)} FROM readings"
    if cond:
        sql += " WHERE " + " AND ".join(cond)
    with connection() as conn:
        return [dict(r) for r in conn.execute(sql + " ORDER BY timestamp, sensor", params)]


def temperature_filters():
    since = normalize_date(request.args.get("since", ""))
    until = normalize_date(request.args.get("until", ""))
    sensors = {s for s in request.args.get("sensors", "").split(",") if s}
    return since, until, sensors


# ---------- windows ----------

def window_row(r):
    return {"id": r["id"], "sensor": r["sensor"], "start_at": r["start_at"], "end_at": r["end_at"] or "", "note": r["note"] or ""}


def get_window(conn, id_):
    r = conn.execute("SELECT * FROM windows WHERE id = ?", (id_,)).fetchone()
    if r is None:
        abort(404, "err.not_found")
    return window_row(r)


def other_window_open(conn, sensor, exclude=""):
    return conn.execute("SELECT 1 FROM windows WHERE sensor = ? AND end_at IS NULL AND id != ?",
                       (sensor, exclude)).fetchone() is not None


def save_window(conn, v):
    conn.execute("INSERT OR REPLACE INTO windows (id, sensor, start_at, end_at, note) VALUES (?, ?, ?, ?, ?)",
                (v["id"], v["sensor"], v["start_at"], v["end_at"] or None, v["note"]))


def thermostat_level(conn, sensor, date, include=True):
    """Thermostat level of a room in effect at a given moment (None if never logged)."""
    r = conn.execute(f"SELECT level FROM thermostats WHERE sensor = ? AND timestamp {'<=' if include else '<'} ? "
                    "ORDER BY timestamp DESC LIMIT 1", (sensor, date)).fetchone()
    return None if r is None else r["level"]


def thermostat_on_open(conn, v):
    """When a window opens, the room's thermostat is set to 0 (unless it already was)."""
    level = thermostat_level(conn, v["sensor"], v["start_at"])
    if level:
        conn.execute("INSERT OR REPLACE INTO thermostats (id, sensor, timestamp, level) VALUES (?, ?, ?, 0)",
                    (uuid.uuid4().hex[:12], v["sensor"], v["start_at"]))


def thermostat_on_close(conn, v):
    """On closing, it goes back to the level it had before opening. If someone changed the thermostat
    by hand while it was open (it is no longer 0), that change is respected."""
    before = thermostat_level(conn, v["sensor"], v["start_at"], include=False)
    if before and thermostat_level(conn, v["sensor"], v["end_at"]) == 0:
        conn.execute("INSERT OR REPLACE INTO thermostats (id, sensor, timestamp, level) VALUES (?, ?, ?, ?)",
                    (uuid.uuid4().hex[:12], v["sensor"], v["end_at"], before))


def latest_manual(ids):
    if not ids:
        return {}
    marks = ", ".join("?" * len(ids))
    sql = f"""SELECT l.sensor, l.temperature, l.humidity, l.timestamp
              FROM readings l JOIN (SELECT sensor, MAX(timestamp) AS m FROM readings
                                    WHERE sensor IN ({marks}) GROUP BY sensor) u
                ON l.sensor = u.sensor AND l.timestamp = u.m"""
    with connection() as conn:
        return {r["sensor"]: {"temperature": r["temperature"], "humidity": r["humidity"], "last_reading": r["timestamp"]}
                for r in conn.execute(sql, list(ids))}


def current_thermostats():
    sql = """SELECT t.* FROM thermostats t
             JOIN (SELECT sensor, MAX(timestamp) AS m FROM thermostats GROUP BY sensor) u
               ON t.sensor = u.sensor AND t.timestamp = u.m"""
    with connection() as conn:
        return {r["sensor"]: {"id": r["id"], "level": r["level"], "since": r["timestamp"]} for r in conn.execute(sql)}


def filtered_thermostats():
    """Level changes in effect during the range (includes the one already active at the start)."""
    since = normalize_date(request.args.get("since", ""))
    until = normalize_date(request.args.get("until", ""))
    sensor = request.args.get("sensor", "")
    cond, params = [], []
    if sensor:
        cond.append("sensor = ?")
        params.append(sensor)
    if until:
        cond.append("timestamp <= ?")
        params.append(until)
    if since:
        cond.append("(until IS NULL OR until >= ?)")
        params.append(since)
    sql = ("SELECT * FROM (SELECT id, sensor, timestamp, level, "
           "LEAD(timestamp) OVER (PARTITION BY sensor ORDER BY timestamp) AS until FROM thermostats)")
    if cond:
        sql += " WHERE " + " AND ".join(cond)
    with connection() as conn:
        return [{**dict(r), "until": r["until"] or ""} for r in conn.execute(sql + " ORDER BY timestamp", params)]


def get_open_windows():
    with connection() as conn:
        return {r["sensor"]: window_row(r) for r in conn.execute("SELECT * FROM windows WHERE end_at IS NULL")}


def apply_changes(win, data):
    if "sensor" in data:
        win["sensor"] = str(data["sensor"] or "")
    if "start_at" in data:
        win["start_at"] = normalize_date(data["start_at"])
    if "end_at" in data:
        win["end_at"] = normalize_date(data["end_at"])
    if "note" in data:
        win["note"] = str(data["note"] or "").strip()[:500]
    if not win.get("sensor"):
        abort(400, "err.missing_room")
    if not win.get("start_at"):
        abort(400, "err.missing_opening")
    if win.get("end_at") and win["end_at"] < win["start_at"]:
        abort(400, "err.end_before_start")
    return win


def window_impact(conn, v):
    """How the room changed while the window was open (readings from the same sensor).

    Compares the last reading up to 60 min before opening with the lowest temperature
    (and the humidity at that moment) between the opening and 30 min after closing, because the room
    keeps cooling for a while. If the window is still open, up to now.
    """
    end_at = (datetime.fromisoformat(v["end_at"]) + timedelta(minutes=30)).isoformat(timespec="seconds") if v["end_at"] else now()
    start_at = datetime.fromisoformat(v["start_at"])
    previous = conn.execute(
        "SELECT timestamp, temperature, humidity FROM readings WHERE sensor = ? AND timestamp <= ? AND timestamp >= ? "
        "AND temperature IS NOT NULL ORDER BY timestamp DESC LIMIT 1",
        (v["sensor"], v["start_at"], (start_at - timedelta(minutes=60)).isoformat(timespec="seconds"))).fetchone()
    approximate = False
    if previous is None:  # no nearby previous reading: use the closest one in time (before or after), up to 3 h
        near = conn.execute(
            "SELECT timestamp, temperature, humidity FROM readings WHERE sensor = ? AND timestamp BETWEEN ? AND ? "
            "AND temperature IS NOT NULL", (v["sensor"], (start_at - timedelta(hours=3)).isoformat(timespec="seconds"),
                                            (start_at + timedelta(hours=3)).isoformat(timespec="seconds"))).fetchall()
        if near:
            previous = min(near, key=lambda r: abs((datetime.fromisoformat(r["timestamp"]) - start_at).total_seconds()))
            approximate = True
    lowest = conn.execute(
        "SELECT timestamp, temperature, humidity FROM readings WHERE sensor = ? AND timestamp > ? AND timestamp <= ? "
        "AND temperature IS NOT NULL ORDER BY temperature ASC, timestamp ASC LIMIT 1",
        (v["sensor"], v["start_at"], end_at)).fetchone()
    if previous is None or lowest is None:
        return None
    # Actual minimum humidity (not the one at the coldest moment). Only with a real previous reading and if the sensor reported during
    # the opening or up to 15 min after closing: if it was down, the stored data repeats an old value.
    hum_drop = None
    if previous["humidity"] is not None and not approximate:
        limit = (datetime.fromisoformat(v["end_at"]) + timedelta(minutes=15)).isoformat(timespec="seconds") if v["end_at"] else now()
        measured = "sensor = ? AND coalesce(last_reading, timestamp) >= ? AND coalesce(last_reading, timestamp) <= ?"
        reported = conn.execute(f"SELECT 1 FROM readings WHERE {measured} LIMIT 1", (v["sensor"], v["start_at"], limit)).fetchone()
        hum = conn.execute(f"SELECT MIN(humidity) AS h FROM readings WHERE {measured} AND humidity IS NOT NULL",
                          (v["sensor"], v["start_at"], end_at)).fetchone()["h"]
        if reported is not None and hum is not None:
            hum_drop = round(previous["humidity"] - hum, 1)
    return {"temp_before": previous["temperature"], "temp_min": lowest["temperature"], "hum_drop": hum_drop,
            "drop": round(previous["temperature"] - lowest["temperature"], 2),
            "hum_before": previous["humidity"], "hum_min": lowest["humidity"],
            "approximate": approximate,
            "minutes_to_min": round((datetime.fromisoformat(lowest["timestamp"]) - datetime.fromisoformat(v["start_at"])).total_seconds() / 60)}


def with_name(v, cfg):
    return {**v, "name": name_of(v["sensor"], cfg)}


def filtered_windows():
    since = normalize_date(request.args.get("since", ""))
    until = normalize_date(request.args.get("until", ""))
    sensor = request.args.get("sensor", "")
    cond, params = [], []
    if sensor:
        cond.append("sensor = ?")
        params.append(sensor)
    if until:
        cond.append("start_at <= ?")
        params.append(until)
    if since:
        cond.append("(end_at IS NULL OR end_at >= ?)")
        params.append(since)
    sql = "SELECT * FROM windows" + (" WHERE " + " AND ".join(cond) if cond else "") + " ORDER BY start_at DESC"
    with connection() as conn:
        return [window_row(r) for r in conn.execute(sql, params)]


# Texts of the exported CSVs: file name, headers and fixed values, per language.
CSV_FILES = {
    "temperatures": {"es": "temperaturas", "en": "temperatures", "de": "temperaturen"},
    "windows": {"es": "ventanas", "en": "windows", "de": "fenster"},
    "thermostats": {"es": "termostatos", "en": "thermostats", "de": "thermostate"},
    "consumption": {"es": "consumo", "en": "consumption", "de": "verbrauch"},
    "meters": {"es": "medidores", "en": "meters", "de": "zaehler"},
    "meter_readings": {"es": "lecturas_medidor", "en": "meter_readings", "de": "zaehlerstaende"},
    "periods": {"es": "periodos", "en": "periods", "de": "zeitraeume"},
    "period_closings": {"es": "cierres_periodo", "en": "period_closings", "de": "periodenabschluesse"},
    "sensor_outages": {"es": "cortes_sensor", "en": "sensor_outages", "de": "sensorausfaelle"},
    "period_bills": {"es": "facturas_periodo", "en": "period_bills", "de": "abrechnungen"},
    "all": {"es": "hausklima_todos_los_datos", "en": "hausklima_all_data", "de": "hausklima_alle_daten"},
}
CSV_COLUMNS = {
    "timestamp": {"es": "Fecha y hora", "en": "Date and time", "de": "Datum und Uhrzeit"},
    "room": {"es": "Habitación", "en": "Room", "de": "Raum"},
    "sensor": {"es": "Sensor", "en": "Sensor", "de": "Sensor"},
    "temperature": {"es": "Temperatura (°C)", "en": "Temperature (°C)", "de": "Temperatur (°C)"},
    "humidity": {"es": "Humedad (%)", "en": "Humidity (%)", "de": "Luftfeuchtigkeit (%)"},
    "battery": {"es": "Batería (%)", "en": "Battery (%)", "de": "Batterie (%)"},
    "linkquality": {"es": "Calidad de enlace", "en": "Link quality", "de": "Verbindungsqualität"},
    "last_reading": {"es": "Última lectura del sensor", "en": "Last sensor reading", "de": "Letzte Sensormessung"},
    "start_at": {"es": "Apertura", "en": "Opened", "de": "Geöffnet"},
    "end_at": {"es": "Cierre", "en": "Closed", "de": "Geschlossen"},
    "note": {"es": "Nota", "en": "Note", "de": "Notiz"},
    "id": {"es": "ID", "en": "ID", "de": "ID"},
    "level": {"es": "Nivel", "en": "Level", "de": "Stufe"},
    "since": {"es": "Desde", "en": "From", "de": "Von"},
    "until": {"es": "Hasta", "en": "To", "de": "Bis"},
    "meter": {"es": "Medidor", "en": "Meter", "de": "Zähler"},
    "unit": {"es": "Unidad", "en": "Unit", "de": "Einheit"},
    "period": {"es": "Período", "en": "Period", "de": "Zeitraum"},
    "days": {"es": "Días", "en": "Days", "de": "Tage"},
    "reading": {"es": "Lectura", "en": "Reading", "de": "Zählerstand"},
    "consumption": {"es": "Consumo", "en": "Consumption", "de": "Verbrauch"},
    "consumption_per_day": {"es": "Consumo por día", "en": "Consumption per day", "de": "Verbrauch pro Tag"},
    "cumulative": {"es": "Acumulado", "en": "Cumulative", "de": "Kumuliert"},
    "reset": {"es": "Medidor reiniciado", "en": "Meter reset", "de": "Zähler zurückgesetzt"},
    "kind": {"es": "Tipo", "en": "Type", "de": "Art"},
    "name": {"es": "Nombre", "en": "Name", "de": "Name"},
    "value": {"es": "Valor", "en": "Value", "de": "Wert"},
    "kwh_home": {"es": "kWh vivienda", "en": "kWh home", "de": "kWh Wohnung"},
    "units_home": {"es": "Unidades vivienda", "en": "Units home", "de": "Einheiten Wohnung"},
    "kwh_building": {"es": "kWh edificio", "en": "kWh building", "de": "kWh Gebäude"},
    "units_building": {"es": "Unidades edificio", "en": "Units building", "de": "Einheiten Gebäude"},
    "price_kwh": {"es": "Precio (€/kWh)", "en": "Price (€/kWh)", "de": "Preis (€/kWh)"},
    "paid": {"es": "Pagado (€)", "en": "Paid (€)", "de": "Bezahlt (€)"},
}
CSV_VALUES = {
    "reading": {"es": "Lectura", "en": "Reading", "de": "Ablesung"},
    "closing": {"es": "Cierre de período", "en": "Period close", "de": "Periodenabschluss"},
    "period_start": {"es": "Inicio de período", "en": "Period start", "de": "Periodenbeginn"},
    "entry": {"es": "Lectura de entrada", "en": "Move-in reading", "de": "Einzugsablesung"},
    "move_in": {"es": "Mudanza", "en": "Move-in", "de": "Einzug"},
    "yes": {"es": "sí", "en": "yes", "de": "ja"},
    "no": {"es": "no", "en": "no", "de": "nein"},
}


def csv_value(key, lang):
    return CSV_VALUES.get(key, {}).get(lang, key)


def csv_text(columns, rows, lang):
    buf = io.StringIO()
    buf.write("﻿")  # BOM: makes Excel open accents and umlauts correctly
    w = csv.writer(buf)
    w.writerow([CSV_COLUMNS.get(c, {}).get(lang, c) for c in columns])
    w.writerows([[f.get(c, "") for c in columns] for f in rows])
    return buf.getvalue()


def csv_response(file, columns, rows, lang):
    buf = io.StringIO(csv_text(columns, rows, lang))
    name = CSV_FILES[file].get(lang, file) + ".csv"
    return Response(buf.getvalue(), mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{name}"'})


# ---------- routes ----------

@app.errorhandler(HTTPException)
def error_json(e):
    return jsonify(error=e.description), e.code


@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


LOGIN_PAGE = """<!doctype html>
<html lang="__LANG__"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__ · __SIGN_IN__</title>
<link rel="icon" type="image/svg+xml" href="/static/favicon.svg">
<link rel="icon" type="image/png" sizes="32x32" href="/static/icon-32.png">
<link rel="apple-touch-icon" sizes="180x180" href="/static/icon-180.png">
<meta name="theme-color" content="#6366f1">
<script>(()=>{let t='auto';try{t=localStorage.getItem('theme')||'auto'}catch(e){}
document.documentElement.setAttribute('data-coreui-theme',t==='auto'?(matchMedia('(prefers-color-scheme: dark)').matches?'dark':'light'):t)})()</script>
<link rel="stylesheet" href="/static/vendor/coreui/coreui.min.css">
<link rel="stylesheet" href="/static/vendor/icons/css/free.min.css">
</head><body>
<div class="bg-body-tertiary min-vh-100 d-flex flex-row align-items-center">
  <div class="container"><div class="row justify-content-center"><div class="col-md-6 col-lg-4">
    <div class="card p-4"><div class="card-body">
      <form method="post" action="/login">
        <h1 class="h3"><i class="cil-home me-2"></i>__TITLE__</h1>
        <p class="text-body-secondary">__SUB__</p>
        <div class="input-group mb-3">
          <span class="input-group-text"><i class="cil-lock-locked"></i></span>
          <input class="form-control" type="password" name="password" placeholder="__PH__" autocomplete="current-password" autofocus required>
        </div>
        __ERROR__
        <button class="btn btn-primary px-4 w-100" type="submit">__SIGN_IN__</button>
      </form>
      <div class="text-center small mt-3">__LANGS__</div>
    </div></div>
  </div></div></div>
</div></body></html>"""


LOGIN_TEXTS = {
    "es": {"TITLE": "Clima de casa", "SIGN_IN": "Entrar", "SUB": "Ingresá la contraseña del panel",
           "PH": "Contraseña", "ERROR": "Contraseña incorrecta"},
    "en": {"TITLE": "Home climate", "SIGN_IN": "Sign in", "SUB": "Enter the dashboard password",
           "PH": "Password", "ERROR": "Wrong password"},
    "de": {"TITLE": "Raumklima", "SIGN_IN": "Anmelden", "SUB": "Passwort für das Dashboard eingeben",
           "PH": "Passwort", "ERROR": "Falsches Passwort"},
}


def login_page(error=False):
    lang = requested_lang()
    txt = LOGIN_TEXTS[lang]
    html = LOGIN_PAGE.replace("__ERROR__", f'<div class="alert alert-danger py-2">{txt["ERROR"]}</div>' if error else "")
    html = html.replace("__LANGS__", " · ".join(
        f'<strong>{i.upper()}</strong>' if i == lang else f'<a href="/login?lang={i}">{i.upper()}</a>' for i in LANGS))
    html = html.replace("__LANG__", lang)
    for key, text in txt.items():
        html = html.replace(f"__{key}__", text)
    resp = Response(html, mimetype="text/html")
    if request.args.get("lang") in LANGS:
        resp.set_cookie("lang", lang, max_age=365 * 24 * 3600, samesite="Lax")
    return resp


def is_authenticated():
    return session.get("auth") == FINGERPRINT


@app.before_request
def require_login():
    if request.path in ("/login", "/logout", "/favicon.ico") or request.path.startswith("/static/") or is_authenticated():
        return None
    if request.path.startswith("/api/"):
        return jsonify(error="err.session"), 401
    return redirect("/login")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if hmac.compare_digest(request.form.get("password", "").encode(), PASSWORD.encode()):
            session.clear()
            session.permanent = True
            session["auth"] = FINGERPRINT
            return redirect("/")
        time.sleep(1)  # slows down brute-force attempts
        return login_page(error=True)
    if is_authenticated():
        return redirect("/")
    return login_page()


@app.get("/favicon.ico")
def favicon():
    return send_from_directory(app.static_folder, "favicon.svg", mimetype="image/svg+xml")


@app.get("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.get("/api/state")
def api_status():
    cfg = read_config()
    manual_sensors = set(cfg["manual_sensors"])
    reading = {**current_readings(), **latest_manual(manual_sensors)}
    open_windows = get_open_windows()
    thermos = current_thermostats()
    with connection() as conn:
        outages = {r["sensor"]: dict(r) for r in conn.execute("SELECT * FROM sensor_outages WHERE until IS NULL")}
    with lock:
        states = dict(sensor_connection)
    ids = set(reading) | set(cfg["rooms"]) | manual_sensors
    sensors = [{"id": s, "name": name_of(s, cfg), "configured_name": cfg["rooms"].get(s, ""),
                 "manual": s in manual_sensors, **reading.get(s, {}), "open_window": open_windows.get(s),
                 "thermostat": thermos.get(s), "connection": "offline" if s in outages else states.get(s),
                 "outage": outages.get(s)} for s in ids]
    sensors.sort(key=lambda s: s["name"].lower())
    with lock:
        a = dict(outdoor)
    return jsonify(sensors=sensors, outdoor=a, place=cfg["place"],
                   interval_minutes=cfg["interval_minutes"], live_seconds=cfg["live_seconds"],
                   humidity_ref=cfg["humidity_ref"], temp_ref=cfg["temp_ref"],
                   move_in_date=cfg["move_in_date"], now=now())


@app.post("/api/save_now")
def api_save_now():
    """Stores the current readings right now (outside the automatic interval)."""
    n = save_readings()
    if not n:
        abort(409, "err.nothing_to_save")
    return jsonify(saved=n, timestamp=now())


# ---------- house metrics ----------

MINUTES_BEFORE_OPENING = 60     # maximum age of the reference reading before opening
MINUTES_AFTER_CLOSING = 15        # the sensor must have reported up to here: otherwise it was down
RECOVERY_MINUTES = 60
MAX_GAP_MINUTES = 90       # do not interpolate between readings further apart


def measured_series(conn, sensor):
    """Actually measured readings: one per 'last_reading', without the copies stored while there is no new data."""
    views, points = set(), []
    for r in conn.execute("SELECT timestamp, temperature, humidity, last_reading FROM readings WHERE sensor = ? "
                         "AND temperature IS NOT NULL ORDER BY timestamp", (sensor,)):
        moment = r["last_reading"] or r["timestamp"]
        if moment not in views:
            views.add(moment)
            points.append((datetime.fromisoformat(moment), r["temperature"], r["humidity"]))
    return sorted(points, key=lambda p: p[0])


def interpolate(series, moment, index):
    """Value of column 'index' at a moment, between the two neighboring readings (None if they are too far apart)."""
    previous = [p for p in series if p[0] <= moment and p[index] is not None]
    following = [p for p in series if p[0] >= moment and p[index] is not None]
    if not previous or not following:
        return None
    a, b = previous[-1], following[0]
    if a[0] == b[0]:
        return a[index]
    if b[0] - a[0] > timedelta(minutes=MAX_GAP_MINUTES):
        return None
    return a[index] + (b[index] - a[index]) * (moment - a[0]) / (b[0] - a[0])


def opening_effect(series, start_at, end_at, next):
    """Humidity and temperature before, at the minimum and one hour later. None if there were no real readings during the opening."""
    previous = [p for p in series if start_at - timedelta(minutes=MINUTES_BEFORE_OPENING) <= p[0] <= start_at]
    during = [p for p in series if start_at < p[0] <= end_at + timedelta(minutes=MINUTES_AFTER_CLOSING)]
    if not previous or not during:
        return None
    before, res = previous[-1], {}
    for key, index in (("hum", 2), ("temp", 1)):
        valid = [p for p in during if p[index] is not None]
        if before[index] is None or not valid:
            continue
        lowest = min(valid, key=lambda p: p[index])
        minutes = (lowest[0] - start_at).total_seconds() / 60
        res[f"{key}_drop"] = before[index] - lowest[index]
        if minutes > 0:
            res[f"{key}_per_min"] = res[f"{key}_drop"] / minutes
        after = lowest[0] + timedelta(minutes=RECOVERY_MINUTES)
        if after <= next:   # another opening would interrupt the recovery
            value = interpolate(series, after, index)
            if value is not None:
                res[f"{key}_recovers"] = value - lowest[index]
    return res


def nightly_drops(series, outdoor, windows):
    """Temperature drop from 23:00 to 06:00, on nights without open windows and with real readings at both ends."""
    res = []
    if not series:
        return res
    day = series[0][0].date()
    while day < series[-1][0].date():
        a = datetime.combine(day, datetime.min.time()) + timedelta(hours=23)
        b = a + timedelta(hours=7)
        day += timedelta(days=1)
        ta, tb = interpolate(series, a, 1), interpolate(series, b, 1)
        if ta is None or tb is None or any(i < b and f > a for i, f in windows):
            continue
        exterior = [p[1] for p in outdoor if a <= p[0] <= b]
        res.append({"drop": ta - tb, "outdoor": statistics.mean(exterior) if exterior else None})
    return res


def median(events, key, decimals=1):
    values = [e[key] for e in events if e.get(key) is not None]
    return round(statistics.median(values), decimals) if values else None


def metrics_summary(openings, nights):
    with_hum = [a for a in openings if a.get("hum_drop") is not None]
    with_temp = [a for a in openings if a.get("temp_drop") is not None]
    return {"n_hum": len(with_hum), "hum_drop": median(openings, "hum_drop"), "hum_per_min": median(openings, "hum_per_min"),
            "hum_recovers": median(openings, "hum_recovers"), "n_hum_recovers": sum(a.get("hum_recovers") is not None for a in openings),
            "n_temp": len(with_temp), "temp_drop": median(openings, "temp_drop"),
            "temp_recovers": median(openings, "temp_recovers"), "n_temp_recovers": sum(a.get("temp_recovers") is not None for a in openings),
            "n_nights": len(nights), "night_drop": median(nights, "drop"), "night_outdoor": median(nights, "outdoor")}


@app.get("/api/metrics")
def api_metrics():
    """How each room behaves, using medians so that a shower or an unusual opening does not distort them."""
    cfg = read_config()
    manual_sensors = set(cfg["manual_sensors"])
    all_openings, all_nights, sensors = [], [], []
    with connection() as conn:
        outdoor = measured_series(conn, OUTDOOR)
        ids = [r["sensor"] for r in conn.execute("SELECT DISTINCT sensor FROM readings") if r["sensor"] != OUTDOOR and r["sensor"] not in manual_sensors]
        for sensor in ids:
            series = measured_series(conn, sensor)
            windows = [(datetime.fromisoformat(r["start_at"]), datetime.fromisoformat(r["end_at"])) for r in
                        conn.execute("SELECT start_at, end_at FROM windows WHERE sensor = ? AND end_at IS NOT NULL ORDER BY start_at", (sensor,))]
            openings = []
            for n, (start_at, end_at) in enumerate(windows):
                next = windows[n + 1][0] if n + 1 < len(windows) else datetime.max
                effect = opening_effect(series, start_at, end_at, next)
                if effect:
                    openings.append(effect)
            nights = nightly_drops(series, outdoor, windows)
            all_openings += openings
            all_nights += nights
            sensors.append({"sensor": sensor, "name": name_of(sensor, cfg), **metrics_summary(openings, nights)})
    sensors.sort(key=lambda s: s["name"].lower())
    return jsonify(sensors=sensors, house=metrics_summary(all_openings, all_nights))


# ---------- home and energy ----------

BILL_COLUMNS = ("kwh_home", "units_home", "kwh_building", "units_building", "price_kwh", "paid")


def heating_meters(conn):
    return conn.execute("SELECT id, name FROM meters WHERE kind = 'heating' ORDER BY name COLLATE NOCASE").fetchall()


def positive(value):
    """Number greater than 0, or None if the field was left empty."""
    if value in ("", None):
        return None
    n = to_number(value)
    if n is None or n <= 0:
        abort(400, "err.home")
    return n


@app.get("/api/home")
def api_home():
    with connection() as conn:
        return jsonify(home=read_config()["home"], periods=read_periods(conn),
                       meters=[dict(m) for m in heating_meters(conn)],
                       bills={r["period"]: dict(r) for r in conn.execute("SELECT * FROM period_bills")})


@app.put("/api/home")
def api_save_home():
    data = request.get_json(silent=True) or {}
    m2_meters = data.get("m2_meters") or {}
    bills = data.get("bills") or {}
    if not isinstance(m2_meters, dict) or not isinstance(bills, dict):
        abort(400, "err.invalid_data")
    people = to_number(data.get("people"), int)
    if data.get("people") not in ("", None) and (people is None or not 1 <= people <= 20):
        abort(400, "err.home")
    energy = data.get("energy") or None
    if energy is not None and energy not in HEIZ_SOURCES:
        abort(400, "err.home")
    home = {"is_apartment": bool(data.get("is_apartment")), "m2_total": positive(data.get("m2_total")),
                "m2_building": positive(data.get("m2_building")),
                "m2_meters": {str(k): positive(v) for k, v in m2_meters.items() if v not in ("", None)},
                "people": people, "energy": energy}
    rows = [(str(p), *(positive((f or {}).get(c)) for c in BILL_COLUMNS)) for p, f in bills.items()]
    with lock, connection() as conn:
        known = {p["id"] for p in read_periods(conn)}
        if any(f[0] not in known for f in rows):
            abort(400, "err.invalid_data")
        for f in rows:
            if any(v is not None for v in f[1:]):
                conn.execute(f"INSERT OR REPLACE INTO period_bills (period, {', '.join(BILL_COLUMNS)}) "
                            f"VALUES ({', '.join('?' * (len(BILL_COLUMNS) + 1))})", f)
            else:
                conn.execute("DELETE FROM period_bills WHERE period = ?", (f[0],))
        cfg = read_config()
        cfg["home"] = home
        save_config(cfg)
    return jsonify(ok=True)


def units_by_period(conn, meters, periods):
    """{period: {meter: units}} counting only since the move-in."""
    res = {p["id"]: {} for p in periods}
    for m in meters:
        for f in calculate_consumption(conn, m["id"], periods):
            if f["period"] in res and f["consumption"] and not f.get("before_move_in"):
                res[f["period"]][m["id"]] = res[f["period"]].get(m["id"], 0) + f["consumption"]
    return res


def per_m2(kwh, m2, hours):
    if kwh is None or not m2:
        return {"kwh_m2": None, "w_m2": None}
    return {"kwh_m2": kwh / m2, "w_m2": kwh * 1000 / m2 / hours if hours else None}


@app.get("/api/energy")
def api_energy():
    """Energy per period. Meter units are not kWh: the annual bill tells how many kWh each one is worth.
    Without a bill (e.g. the current period) the weighted average of previous bills is used (accumulated kWh ÷ units)
    and the result is marked as estimated. For an apartment the building totals are preferred: they cover
    full years, while the home may have a partial year because of the move-in."""
    cfg = read_config()
    home = cfg["home"]
    m2_meters = home["m2_meters"]
    with connection() as conn:
        periods = read_periods(conn)
        meters = heating_meters(conn)
        bills = {r["period"]: dict(r) for r in conn.execute("SELECT * FROM period_bills")}
        units = units_by_period(conn, meters, periods)
    is_apt = home["is_apartment"]
    m2_home = home["m2_total"] or sum(v for k, v in m2_meters.items() if any(m["id"] == k for m in meters)) or None
    now_dt = datetime.now()
    move_in = datetime.fromisoformat(cfg["move_in_date"]) if cfg["move_in_date"] else None
    rows, last_price, kwh_cumulative, cumulative_units = [], None, 0.0, 0.0
    for p in periods:
        f = bills.get(p["id"], {})
        panel = sum(units[p["id"]].values())
        start, end_at = period_limits(p)
        if start > now_dt:
            continue
        hours = max((min(end_at, now_dt) - max(start, move_in or start)).total_seconds() / 3600, 0)
        before_move_in = bool(move_in and end_at < move_in)      # no own consumption; building data does exist
        partial = bool(move_in and start < move_in <= end_at)
        base = f.get("units_home") or panel
        factor, source = None, None
        if f.get("kwh_home") and base:
            factor, source = f["kwh_home"] / base, "home"
        elif is_apt and f.get("kwh_building") and f.get("units_building"):
            factor, source = f["kwh_building"] / f["units_building"], "building"
        estimated = factor is None
        if estimated:
            factor = kwh_cumulative / cumulative_units if cumulative_units else None
        elif is_apt and f.get("kwh_building") and f.get("units_building"):
            kwh_cumulative += f["kwh_building"]
            cumulative_units += f["units_building"]
        else:
            kwh_cumulative += factor * base
            cumulative_units += base
        kwh = f["kwh_home"] if source == "home" else (panel * factor if factor and not before_move_in else None)
        price = f.get("price_kwh") or last_price
        last_price = price
        row = {"id": p["id"], "name": p["name"], "start_at": p["start_at"], "end_at": p["end_at"], "units": panel, "billed_units": f.get("units_home"),
                "factor": factor, "source": source, "estimated": estimated, "kwh": kwh, **per_m2(kwh, m2_home, hours), "building": None,
                "price": price, "price_estimated": bool(price) and not f.get("price_kwh"),
                "cost": kwh * price if kwh is not None and price else None, "paid": f.get("paid")}
        if is_apt and f.get("kwh_building"):
            row["building"] = {"kwh": f["kwh_building"], **per_m2(f["kwh_building"], home["m2_building"], hours)}
            # With a mid-period move-in, the home counts only part and the building the full year
            if row["kwh_m2"] and row["building"]["kwh_m2"] and not partial:
                row["building"]["difference"] = (row["kwh_m2"] / row["building"]["kwh_m2"] - 1) * 100
        rows.append({**row, "hours": hours, "in_progress": end_at >= now_dt, "partial_move_in": partial, "per_meter": {
            m["id"]: {"units": units[p["id"]].get(m["id"], 0), "kwh": units[p["id"]].get(m["id"], 0) * factor if factor else None,
                      **per_m2(units[p["id"]].get(m["id"], 0) * factor if factor else None, m2_meters.get(m["id"]), hours)}
            for m in meters}})
    return jsonify(is_apartment=is_apt, energy=home.get("energy"), m2_home=m2_home, m2_building=home["m2_building"], periods=rows,
                   meters=[{**dict(m), "m2": m2_meters.get(m["id"])} for m in meters])


@app.get("/api/live")
def api_live():
    """Lightweight query: the browser requests /api/state only when this version changes."""
    return jsonify(version=live_version)


@app.get("/api/daily_weather")
def api_daily_weather():
    with connection() as conn:
        return jsonify([[r["date"], r["mean_temp"]] for r in conn.execute("SELECT * FROM daily_weather ORDER BY date")])


@app.get("/api/outages")
def api_outages():
    """Log of sensor connection outages, most recent first."""
    cfg = read_config()
    limit = to_number(request.args.get("limit"), int) or 200
    with connection() as conn:
        rows = conn.execute("SELECT * FROM sensor_outages ORDER BY since DESC LIMIT ?", (min(max(limit, 1), 1000),)).fetchall()
    return jsonify([{**dict(r), "name": name_of(r["sensor"], cfg)} for r in rows])


@app.get("/api/temperatures")
def api_temperatures():
    rows = [[f[c] for c in TEMP_COLS] for f in read_temperatures(*temperature_filters())]
    return jsonify(columns=TEMP_COLS, rows=rows)


@app.get("/api/temperatures.csv")
def api_temperatures_csv():
    cfg = read_config()
    lang = requested_lang()
    rows = [{**f, "room": name_of(f["sensor"], cfg, lang)} for f in read_temperatures(*temperature_filters())]
    return csv_response("temperatures", ["timestamp", "room"] + TEMP_COLS[1:], rows, lang)


@app.get("/api/windows")
def api_windows():
    cfg = read_config()
    with connection() as conn:
        return jsonify([{**with_name(v, cfg), "impact": window_impact(conn, v)} for v in filtered_windows()])


@app.get("/api/windows.csv")
def api_windows_csv():
    cfg = read_config()
    lang = requested_lang()
    rows = [{**v, "room": name_of(v["sensor"], cfg, lang)} for v in filtered_windows()]
    return csv_response("windows", ["room", "start_at", "end_at", "note", "sensor", "id"], rows, lang)


@app.post("/api/windows")
def api_create_window():
    data = request.get_json(silent=True) or {}
    data.setdefault("start_at", now())
    data.setdefault("end_at", "")
    v = apply_changes({"id": uuid.uuid4().hex[:12], "note": ""}, data)
    with lock, connection() as conn:
        if not v["end_at"] and other_window_open(conn, v["sensor"]):
            abort(409, "err.already_open")
        save_window(conn, v)
        if not v["end_at"]:
            thermostat_on_open(conn, v)
    if "start_at" not in (request.get_json(silent=True) or {}):
        read_instantly(v["start_at"])  # the reading at opening time is the reference (max.)
    return jsonify(with_name(v, read_config())), 201


@app.put("/api/windows/<id_>")
def api_edit_window(id_):
    data = request.get_json(silent=True) or {}
    with lock, connection() as conn:
        v = apply_changes(get_window(conn, id_), data)
        if not v["end_at"] and other_window_open(conn, v["sensor"], exclude=id_):
            abort(409, "err.already_open")
        save_window(conn, v)
    return jsonify(with_name(v, read_config()))


@app.post("/api/windows/<id_>/close")
def api_close_window(id_):
    with lock, connection() as conn:
        v = get_window(conn, id_)
        closed_now = not v["end_at"]
        if closed_now:
            v["end_at"] = max(now(), v["start_at"])
            save_window(conn, v)
            thermostat_on_close(conn, v)
    if closed_now:
        read_instantly(v["end_at"])
    return jsonify(with_name(v, read_config()))


@app.delete("/api/windows/<id_>")
def api_delete_window(id_):
    with lock, connection() as conn:
        get_window(conn, id_)
        conn.execute("DELETE FROM windows WHERE id = ?", (id_,))
    return jsonify(ok=True)


# ---------- thermostats ----------

@app.get("/api/thermostats")
def api_thermostats():
    cfg = read_config()
    return jsonify([{**t, "name": name_of(t["sensor"], cfg)} for t in filtered_thermostats()])


@app.get("/api/thermostats.csv")
def api_thermostats_csv():
    cfg = read_config()
    lang = requested_lang()
    rows = [{**t, "room": name_of(t["sensor"], cfg, lang), "since": t["timestamp"]} for t in filtered_thermostats()]
    return csv_response("thermostats", ["room", "level", "since", "until", "sensor", "id"], rows, lang)


@app.post("/api/thermostats")
def api_create_thermostat():
    data = request.get_json(silent=True) or {}
    sensor = str(data.get("sensor") or "")
    if not sensor:
        abort(400, "err.missing_room")
    level = to_number(data.get("level"))
    if level not in VALID_LEVELS:
        abort(400, "err.invalid_level")
    date = normalize_date(data.get("timestamp")) or now()
    with lock, connection() as conn:
        previous = conn.execute("SELECT level FROM thermostats WHERE sensor = ? AND timestamp <= ? "
                             "ORDER BY timestamp DESC LIMIT 1", (sensor, date)).fetchone()
        if previous is not None and previous["level"] == level:
            return jsonify(unchanged=True)
        id_ = uuid.uuid4().hex[:12]
        # A single change per room and moment: if there was already one at that time, it is replaced
        conn.execute("INSERT OR REPLACE INTO thermostats (id, sensor, timestamp, level) VALUES (?, ?, ?, ?)",
                    (id_, sensor, date, level))
    return jsonify(id=id_, sensor=sensor, timestamp=date, level=level), 201


@app.put("/api/thermostats/<id_>")
def api_edit_thermostat(id_):
    """Corrects the level and/or date of an already logged thermostat change (the room does not change)."""
    data = request.get_json(silent=True) or {}
    level = to_number(data.get("level"))
    if level not in VALID_LEVELS:
        abort(400, "err.invalid_level")
    date = normalize_date(data.get("timestamp")) or abort(400, "err.invalid_date")
    with lock, connection() as conn:
        row = conn.execute("SELECT sensor FROM thermostats WHERE id = ?", (id_,)).fetchone()
        if row is None:
            abort(404, "err.not_found")
        if conn.execute("SELECT 1 FROM thermostats WHERE sensor = ? AND timestamp = ? AND id <> ?",
                       (row["sensor"], date, id_)).fetchone():
            abort(409, "err.duplicate_thermo")
        conn.execute("UPDATE thermostats SET level = ?, timestamp = ? WHERE id = ?", (level, date, id_))
    return jsonify(id=id_, sensor=row["sensor"], timestamp=date, level=level)


@app.delete("/api/thermostats/<id_>")
def api_delete_thermostat(id_):
    with connection() as conn:
        if not conn.execute("DELETE FROM thermostats WHERE id = ?", (id_,)).rowcount:
            abort(404, "err.not_found")
    return jsonify(ok=True)


# ---------- heating meters and consumption ----------

def read_periods(conn):
    return [dict(r) for r in conn.execute("SELECT * FROM periods ORDER BY start_at")]


def period_limits(p):
    """Start (00:00 of the first day) and end (23:59:59 of the last day) of a period."""
    return datetime.fromisoformat(p["start_at"]), datetime.fromisoformat(p["end_at"]) + timedelta(days=1, seconds=-1)


def period_of(periods, date):
    for p in periods:
        start, end_at = period_limits(p)
        if start <= date <= end_at:
            return p
    return None


def calculate_consumption(conn, meter, periods=None):
    """Readings in order with the consumption since the previous point and the cumulative total.

    Without periods: the first reading is the baseline and a counter that goes down is treated as a reset.
    With billing periods, calculated ("virtual") points are added:
        - the start of each period with the counter at 0 (the reading counts as consumption from that day);
        - the closing of each period with its final value, if entered.
    If a period starts without the previous closing being known, that segment is marked as missing data.
    """
    if periods is None:
        periods = read_periods(conn)
    meter_kind = conn.execute("SELECT kind FROM meters WHERE id = ?", (meter,)).fetchone()
    if meter_kind is not None and meter_kind["kind"] != "heating":
        periods = []      # water counters do not go back to 0 when the period changes
    closings = {r["period"]: r["value"] for r in conn.execute("SELECT period, value FROM period_closings WHERE meter = ?", (meter,))}
    events = []   # (date, order within the same instant, value, kind, reading row, period)
    for r in conn.execute("SELECT * FROM meter_readings WHERE meter = ? ORDER BY timestamp", (meter,)):
        f = datetime.fromisoformat(r["timestamp"])
        events.append((f, 1, r["value"], "reading", r, period_of(periods, f)))
    for p in periods:
        if p["id"] in closings:
            events.append((period_limits(p)[1], 2, closings[p["id"]], "closing", None, p))
    if periods and events:
        first, last = min(e[0] for e in events), max(e[0] for e in events)
        for p in periods:
            start, end_at = period_limits(p)
            if end_at >= first and start <= last:      # only the periods touching this meter's data
                events.append((start, 0, 0.0, "period_start", None, p))
    events.sort(key=lambda e: (e[0], e[1]))

    rows, previous, cumulative = [], None, 0.0
    for date, _, value, kind, r, p in events:
        consumption = days = None
        auto_reset = missing_closing = False
        if previous is not None:
            days = (date - previous["date"]).total_seconds() / 86400
            if kind == "period_start":
                consumption = 0.0
                # what was consumed between the last datum and the end of the previous period is unknown
                missing_closing = previous["kind"] != "closing"
            elif r is not None and r["reset"]:
                consumption = value
            elif value < previous["value"]:
                consumption, auto_reset = value, True
            else:
                consumption = value - previous["value"]
            cumulative += consumption
        rows.append({
            "id": r["id"] if r is not None else None, "meter": meter, "kind": kind, "virtual": r is None,
            "timestamp": date.isoformat(timespec="seconds"), "value": value,
            "reset": bool(r["reset"]) if r is not None else False, "auto_reset": auto_reset,
            "missing_closing": missing_closing, "note": r["note"] if r is not None else "",
            "period": p["id"] if p else "", "period_name": p["name"] if p else "",
            "since": previous["date"].isoformat(timespec="seconds") if previous is not None else "",
            "days": days, "consumption": consumption, "cumulative": cumulative,
        })
        previous = {"date": date, "value": value, "kind": kind}
    return apply_move_in(rows, read_config()["move_in_date"])


def apply_move_in(rows, move_in):
    """Subtracts what was consumed before the move-in date (it belonged to the previous tenant).

    If there is a reading that same day, it is the "move-in reading" and the calculation is exact; otherwise the value
    at move-in is estimated by splitting the segment between the previous and next readings evenly.
    """
    if not move_in or not rows:
        return rows
    m = datetime.fromisoformat(move_in)
    dates = [datetime.fromisoformat(f["timestamp"]) for f in rows]
    # a real reading on that same day (at any time) is the move-in reading: the calculation is exact
    same_day = [fe for fe, f in zip(dates, rows) if not f["virtual"] and fe.date() == m.date()]
    if same_day:
        m = same_day[0]
    if m <= dates[0] and not same_day:
        return rows                       # all data is after the move-in
    exact = m in dates
    if exact:
        base = rows[dates.index(m)]
        cum_m, value_m = base["cumulative"], base["value"]
    elif m >= dates[-1]:
        cum_m, value_m = rows[-1]["cumulative"], None
    else:
        i = max(k for k, f in enumerate(dates) if f < m)
        p, q = rows[i], rows[i + 1]
        frac = (m - dates[i]) / (dates[i + 1] - dates[i])
        cum_m = p["cumulative"] + (q["cumulative"] - p["cumulative"]) * frac
        # the counter value can only be estimated if it was not reset in that segment
        same_segment = q["kind"] == "reading" and not q["reset"] and not q["auto_reset"] or q["kind"] == "closing"
        value_m = p["value"] + (q["value"] - p["value"]) * frac if same_segment else None

    res, last = [], None
    for f, date in zip(rows, dates):
        f = dict(f)
        if date < m or (exact and date == m):
            if date == m:
                f.update(kind="entry" if not f["virtual"] else f["kind"], consumption=None, days=None)
            else:
                f["before_move_in"] = True
            f["cumulative"] = 0.0
            res.append(f)
            last = date
            continue
        if last is not None and last < m and not exact:
            # calculated point at the moment of the move-in
            res.append({**f, "id": None, "kind": "move_in", "virtual": True, "estimated": True,
                        "timestamp": m.isoformat(timespec="seconds"), "value": value_m, "consumption": None, "days": None,
                        "reset": False, "auto_reset": False, "missing_closing": False, "note": "", "since": "", "cumulative": 0.0})
        if last is not None and last <= m:     # first segment after the move-in: counts only from that day
            f["since"] = m.isoformat(timespec="seconds")
            f["days"] = (date - m).total_seconds() / 86400
            f["consumption"] = 0.0 if f["kind"] == "period_start" else f["cumulative"] - cum_m
        f["cumulative"] = max(0.0, f["cumulative"] - cum_m)
        res.append(f)
        last = date
    return res


def read_meters(conn):
    res = []
    periods = read_periods(conn)
    current = period_of(periods, datetime.now())
    for m in conn.execute("SELECT id, name, sensor, unit, kind FROM meters ORDER BY name COLLATE NOCASE"):
        rows = calculate_consumption(conn, m["id"], periods)
        measured = [f for f in rows if not f["virtual"]]
        last = measured[-1] if measured else None
        res.append({**dict(m), "readings": len(measured), "last": last and {"value": last["value"], "timestamp": last["timestamp"]},
                    "cumulative": rows[-1]["cumulative"] if rows else 0,
                    "current_period": current["name"] if current and m["kind"] == "heating" else ""})
    return res


def meter_data(data, current=None):
    m = dict(current or {"sensor": "", "unit": "", "kind": "heating"})
    if "kind" in data:
        if data["kind"] not in METER_KINDS:
            abort(400, "err.invalid_data")
        # The kind is chosen when creating the meter; afterwards a water counter can only switch between hot and cold
        if current is None or (m["kind"] in WATER_KINDS and data["kind"] in WATER_KINDS):
            m["kind"] = data["kind"]
        elif data["kind"] != m["kind"]:
            abort(400, "err.invalid_data")
    if "name" in data:
        m["name"] = str(data["name"] or "").strip()[:40]
    if "sensor" in data:
        m["sensor"] = str(data["sensor"] or "")
    if "unit" in data:
        m["unit"] = str(data["unit"] or "").strip()[:15]
    if not m.get("name"):
        abort(400, "err.missing_name")
    return m


@app.get("/api/meters")
def api_meters():
    with connection() as conn:
        return jsonify(read_meters(conn))


@app.post("/api/meters")
def api_create_meter():
    m = meter_data(request.get_json(silent=True) or {})
    m["id"] = "med_" + uuid.uuid4().hex[:8]
    with connection() as conn:
        conn.execute("INSERT INTO meters (id, name, sensor, unit, kind) VALUES (:id, :name, :sensor, :unit, :kind)", m)
    return jsonify(m), 201


@app.put("/api/meters/<id_>")
def api_edit_meter(id_):
    with connection() as conn:
        current = conn.execute("SELECT * FROM meters WHERE id = ?", (id_,)).fetchone()
        if current is None:
            abort(404, "err.not_found")
        m = meter_data(request.get_json(silent=True) or {}, dict(current))
        conn.execute("UPDATE meters SET name = :name, sensor = :sensor, unit = :unit, kind = :kind WHERE id = :id", m)
    return jsonify(m)


@app.delete("/api/meters/<id_>")
def api_delete_meter(id_):
    with connection() as conn:
        if not conn.execute("DELETE FROM meters WHERE id = ?", (id_,)).rowcount:
            abort(404, "err.not_found")
        n = conn.execute("DELETE FROM meter_readings WHERE meter = ?", (id_,)).rowcount
        conn.execute("DELETE FROM period_closings WHERE meter = ?", (id_,))
    return jsonify(ok=True, deleted_readings=n)


@app.get("/api/meter_readings")
def api_meter_readings():
    """All readings with calculated consumption (date filtering happens in the page)."""
    meter = request.args.get("meter", "")
    with connection() as conn:
        periods = read_periods(conn)
        ids = [meter] if meter else [r["id"] for r in conn.execute("SELECT id FROM meters")]
        return jsonify([f for m in ids for f in calculate_consumption(conn, m, periods)])


@app.post("/api/meter_readings")
def api_create_meter_reading():
    data = request.get_json(silent=True) or {}
    meter = str(data.get("meter") or "")
    value = to_number(data.get("value"))
    if value is None or value < 0:
        abort(400, "err.invalid_reading")
    date = normalize_date(data.get("timestamp")) or now()
    with connection() as conn:
        if conn.execute("SELECT 1 FROM meters WHERE id = ?", (meter,)).fetchone() is None:
            abort(400, "err.not_found")
        id_ = uuid.uuid4().hex[:12]
        conn.execute("INSERT OR REPLACE INTO meter_readings (id, meter, timestamp, value, reset, note) VALUES (?, ?, ?, ?, ?, ?)",
                    (id_, meter, date, value, 1 if data.get("reset") else 0, str(data.get("note") or "").strip()[:200]))
    return jsonify(id=id_, meter=meter, timestamp=date, value=value), 201


@app.put("/api/meter_readings/<id_>")
def api_edit_meter_reading(id_):
    data = request.get_json(silent=True) or {}
    with connection() as conn:
        r = conn.execute("SELECT * FROM meter_readings WHERE id = ?", (id_,)).fetchone()
        if r is None:
            abort(404, "err.not_found")
        v = dict(r)
        if "value" in data:
            v["value"] = to_number(data["value"])
            if v["value"] is None or v["value"] < 0:
                abort(400, "err.invalid_reading")
        if "timestamp" in data:
            v["timestamp"] = normalize_date(data["timestamp"]) or abort(400, "err.invalid_date")
        if "reset" in data:
            v["reset"] = 1 if data["reset"] else 0
        if "note" in data:
            v["note"] = str(data["note"] or "").strip()[:200]
        conn.execute("UPDATE meter_readings SET timestamp = :timestamp, value = :value, reset = :reset, "
                    "note = :note WHERE id = :id", v)
    return jsonify(v)


@app.delete("/api/meter_readings/<id_>")
def api_delete_meter_reading(id_):
    with connection() as conn:
        if not conn.execute("DELETE FROM meter_readings WHERE id = ?", (id_,)).rowcount:
            abort(404, "err.not_found")
    return jsonify(ok=True)


def period_data(data, current=None):
    p = dict(current or {})
    if "name" in data:
        p["name"] = str(data["name"] or "").strip()[:40]
    for field in ("start_at", "end_at"):
        if field in data:
            try:
                p[field] = datetime.strptime(str(data[field]), "%Y-%m-%d").date().isoformat()
            except ValueError:
                abort(400, "err.invalid_date")
    if not p.get("name"):
        abort(400, "err.missing_name")
    if not p.get("start_at") or not p.get("end_at"):
        abort(400, "err.invalid_date")
    if p["end_at"] < p["start_at"]:
        abort(400, "err.period_dates")
    return p


def validate_overlap(conn, p):
    other = conn.execute("SELECT name FROM periods WHERE id != ? AND start_at <= ? AND end_at >= ?",
                       (p.get("id", ""), p["end_at"], p["start_at"])).fetchone()
    if other is not None:
        abort(409, "err.overlapping_period")


@app.get("/api/periods")
def api_periods():
    with connection() as conn:
        return jsonify(periods=read_periods(conn),
                       closings=[dict(r) for r in conn.execute("SELECT * FROM period_closings")])


@app.post("/api/periods")
def api_create_period():
    p = period_data(request.get_json(silent=True) or {})
    p["id"] = "per_" + uuid.uuid4().hex[:8]
    with lock, connection() as conn:
        validate_overlap(conn, p)
        conn.execute("INSERT INTO periods (id, name, start_at, end_at) VALUES (:id, :name, :start_at, :end_at)", p)
    return jsonify(p), 201


@app.put("/api/periods/<id_>")
def api_edit_period(id_):
    with lock, connection() as conn:
        current = conn.execute("SELECT * FROM periods WHERE id = ?", (id_,)).fetchone()
        if current is None:
            abort(404, "err.not_found")
        p = period_data(request.get_json(silent=True) or {}, dict(current))
        validate_overlap(conn, p)
        conn.execute("UPDATE periods SET name = :name, start_at = :start_at, end_at = :end_at WHERE id = :id", p)
    return jsonify(p)


@app.delete("/api/periods/<id_>")
def api_delete_period(id_):
    with connection() as conn:
        if not conn.execute("DELETE FROM periods WHERE id = ?", (id_,)).rowcount:
            abort(404, "err.not_found")
        conn.execute("DELETE FROM period_closings WHERE period = ?", (id_,))
        conn.execute("DELETE FROM period_bills WHERE period = ?", (id_,))
    return jsonify(ok=True)


@app.put("/api/periods/<id_>/closings")
def api_period_closings(id_):
    """Final period values per meter: {"values": {"med_x": 1605, "med_y": ""}} ('' deletes)."""
    values = (request.get_json(silent=True) or {}).get("values") or {}
    if not isinstance(values, dict):
        abort(400, "err.invalid_data")
    with connection() as conn:
        if conn.execute("SELECT 1 FROM periods WHERE id = ?", (id_,)).fetchone() is None:
            abort(404, "err.not_found")
        for meter, value in values.items():
            if value in ("", None):
                conn.execute("DELETE FROM period_closings WHERE meter = ? AND period = ?", (meter, id_))
                continue
            v = to_number(value)
            if v is None or v < 0:
                abort(400, "err.invalid_reading")
            conn.execute("INSERT OR REPLACE INTO period_closings (meter, period, value) VALUES (?, ?, ?)", (meter, id_, v))
    return jsonify(ok=True)


@app.get("/api/consumption.csv")
def api_consumption_csv():
    since = normalize_date(request.args.get("since", ""))
    until = normalize_date(request.args.get("until", ""))
    meter = request.args.get("meter", "")
    kinds = {k for k in request.args.get("kind", "").split(",") if k}   # one kind or several, comma separated
    cfg, lang = read_config(), requested_lang()
    rows = []
    with connection() as conn:
        for m in read_meters(conn):
            if (meter and m["id"] != meter) or (kinds and m["kind"] not in kinds):
                continue
            for f in calculate_consumption(conn, m["id"]):
                if f["consumption"] is None or f["kind"] == "period_start" or f.get("before_move_in") or \
                        (since and f["timestamp"] < since) or (until and f["since"] > until):
                    continue
                rows.append({"meter": m["name"], "room": name_of(m["sensor"], cfg, lang) if m["sensor"] else "",
                              "unit": m["unit"], "since": f["since"], "until": f["timestamp"],
                              "days": round(f["days"], 2), "reading": f["value"], "consumption": round(f["consumption"], 3),
                              "consumption_per_day": round(f["consumption"] / f["days"], 3) if f["days"] else "",
                              "cumulative": round(f["cumulative"], 3),
                              "reset": csv_value("yes" if f["reset"] or f["auto_reset"] else "no", lang),
                              "kind": csv_value(f["kind"], lang), "period": f["period_name"]})
    return csv_response("consumption", ["meter", "room", "unit", "period", "since", "until", "days", "reading", "consumption",
                                     "consumption_per_day", "cumulative", "reset", "kind"], rows, lang)


@app.get("/api/all.zip")
def api_export_zip():
    """All database tables, unfiltered, in a ZIP with one CSV per table."""
    cfg, lang = read_config(), requested_lang()
    tables = [("temperatures", "readings"), ("windows", "windows"), ("thermostats", "thermostats"),
              ("meters", "meters"), ("meter_readings", "meter_readings"),
              ("periods", "periods"), ("period_closings", "period_closings"), ("sensor_outages", "sensor_outages"),
              ("period_bills", "period_bills")]
    order = {"readings": "timestamp, sensor", "windows": "start_at", "thermostats": "sensor, timestamp",
             "meter_readings": "meter, timestamp", "periods": "start_at", "sensor_outages": "since"}
    output = io.BytesIO()
    with connection() as conn, zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as z:
        for file, table in tables:
            cur = conn.execute(f"SELECT * FROM {table}" + (f" ORDER BY {order[table]}" if table in order else ""))
            columns = [c[0] for c in cur.description]
            rows = [dict(zip(columns, r)) for r in cur]
            if "sensor" in columns:
                for f in rows:
                    f["room"] = name_of(f["sensor"], cfg, lang) if f["sensor"] else ""
                columns.insert(columns.index("sensor"), "room")
            z.writestr(CSV_FILES[file].get(lang, file) + ".csv", csv_text(columns, rows, lang))
    return Response(output.getvalue(), mimetype="application/zip",
                    headers={"Content-Disposition": f'attachment; filename="{CSV_FILES["all"][lang]}.zip"'})


# ---------- manual sensors ----------

def save_config(cfg):
    write_atomic(CONFIG_PATH, json.dumps(cfg, ensure_ascii=False, indent=2))


def require_manual(sensor, cfg):
    if sensor not in cfg["manual_sensors"]:
        abort(400, "err.not_manual")


@app.post("/api/manual_sensors")
def api_create_manual():
    name = str((request.get_json(silent=True) or {}).get("name") or "").strip()[:40]
    if not name:
        abort(400, "err.missing_name")
    with lock:
        cfg = read_config()
        id_ = "manual_" + uuid.uuid4().hex[:8]
        cfg["manual_sensors"].append(id_)
        cfg["rooms"][id_] = name
        save_config(cfg)
    return jsonify(id=id_, name=name), 201


@app.delete("/api/manual_sensors/<id_>")
def api_delete_manual(id_):
    """Deletes the manual sensor together with its records and window openings."""
    with lock:
        cfg = read_config()
        require_manual(id_, cfg)
        with connection() as conn:
            n = conn.execute("DELETE FROM readings WHERE sensor = ?", (id_,)).rowcount
            conn.execute("DELETE FROM windows WHERE sensor = ?", (id_,))
            conn.execute("DELETE FROM thermostats WHERE sensor = ?", (id_,))
        cfg["manual_sensors"].remove(id_)
        cfg["rooms"].pop(id_, None)
        save_config(cfg)
    return jsonify(ok=True, records_deleted=n)


def move_history(conn, source, target):
    """Moves readings, windows and thermostats from one sensor to another. If the target already has a record at the
    same moment, the target's record is kept."""
    for table in ("readings", "thermostats"):
        conn.execute(f"UPDATE OR IGNORE {table} SET sensor = ? WHERE sensor = ?", (target, source))
        conn.execute(f"DELETE FROM {table} WHERE sensor = ?", (source,))
    conn.execute("UPDATE windows SET sensor = ? WHERE sensor = ?", (target, source))


@app.post("/api/manual_sensors/<id_>/automatic")
def api_manual_to_automatic(id_):
    """Links a manual sensor to a Zigbee sensor: history and name move to the Zigbee sensor."""
    target = str((request.get_json(silent=True) or {}).get("target") or "")
    with lock:
        known = set(ieee_by_name.values())
    known |= set(current_readings())  # outside the lock: current_readings acquires it
    with lock:
        cfg = read_config()
        require_manual(id_, cfg)
        if (not target or target == OUTDOOR or target in cfg["manual_sensors"]
                or target not in known | set(cfg["rooms"])):
            abort(400, "err.invalid_target")
        with connection() as conn:
            move_history(conn, id_, target)
        cfg["rooms"][target] = cfg["rooms"].pop(id_, "Manual")
        cfg["manual_sensors"].remove(id_)
        save_config(cfg)
    return jsonify(ok=True, id=target)


@app.post("/api/manual_sensors/convert/<id_>")
def api_automatic_to_manual(id_):
    """Converts a Zigbee sensor to manual: history and name move to a new manual sensor."""
    with lock:
        cfg = read_config()
        if id_ == OUTDOOR or id_ in cfg["manual_sensors"]:
            abort(400, "err.not_automatic")
        new = "manual_" + uuid.uuid4().hex[:8]
        with connection() as conn:
            move_history(conn, id_, new)
        cfg["rooms"][new] = cfg["rooms"].pop(id_, f"Sensor {id_[-4:]}")
        cfg["manual_sensors"].append(new)
        save_config(cfg)
    return jsonify(ok=True, id=new)


@app.post("/api/manual_readings")
def api_create_manual_reading():
    data = request.get_json(silent=True) or {}
    sensor = str(data.get("sensor") or "")
    require_manual(sensor, read_config())
    date = normalize_date(data.get("timestamp")) or abort(400, "err.invalid_date")
    temperature = to_number(data.get("temperature"))
    humidity = to_number(data.get("humidity"))
    if temperature is None or not -50 <= temperature <= 80:
        abort(400, "err.temp_invalid")
    if humidity is not None and not 0 <= humidity <= 100:
        abort(400, "err.invalid_humidity")
    with connection() as conn:
        conn.execute("INSERT OR REPLACE INTO readings (timestamp, sensor, temperature, humidity, last_reading) "
                    "VALUES (?, ?, ?, ?, ?)", (date, sensor, temperature, humidity, date))
    return jsonify(timestamp=date, sensor=sensor, temperature=temperature, humidity=humidity), 201


@app.delete("/api/manual_readings")
def api_delete_manual_reading():
    sensor = request.args.get("sensor", "")
    require_manual(sensor, read_config())
    date = normalize_date(request.args.get("timestamp", ""))
    with connection() as conn:
        n = conn.execute("DELETE FROM readings WHERE sensor = ? AND timestamp = ?", (sensor, date)).rowcount
    if not n:
        abort(404, "err.not_found")
    return jsonify(ok=True)


def validate_benchmark(data):
    """Table with limits A, B and C for 1 to 5 people, per home kind; each limit greater than the previous one."""
    if not isinstance(data, dict) or set(data) != {"house", "apartment"}:
        abort(400, "err.water_benchmark")
    table = {}
    for kind, rows in data.items():
        if not isinstance(rows, list) or len(rows) != 5:
            abort(400, "err.water_benchmark")
        table[kind] = []
        for row in rows:
            if not isinstance(row, list) or len(row) != 3:
                abort(400, "err.water_benchmark")
            a, b, c = (to_number(v) for v in row)
            if None in (a, b, c) or not 0 < a < b < c <= 10000:
                abort(400, "err.water_benchmark")
            table[kind].append([a, b, c])
    return table


def validate_benchmarks(data):
    """{year: table}, with at least one year."""
    if not isinstance(data, dict) or not data:
        abort(400, "err.water_benchmark")
    if not all(str(a).isdigit() and 2000 <= int(a) <= 2100 for a in data):
        abort(400, "err.benchmark_year")
    return {str(a): validate_benchmark(t) for a, t in data.items()}


def heiz_limits(values):
    if not isinstance(values, list) or len(values) != 3:
        abort(400, "err.heizspiegel")
    a, b, c = (to_number(v) for v in values)
    if None in (a, b, c) or not 0 < a < b < c <= 10000:
        abort(400, "err.heizspiegel")
    return [a, b, c]


def validate_heizspiegel(data):
    """One edition: ascending floor-area bands, the last one unbounded, with limits A, B and C for each source."""
    if not isinstance(data, dict) or not isinstance(data.get("bands"), list) or not 1 <= len(data["bands"]) <= 8:
        abort(400, "err.heizspiegel")
    edition, min_m2 = to_number(data.get("edition"), int), to_number(data.get("min_m2"))
    if edition is None or not 2000 <= edition <= 2100 or min_m2 is None or min_m2 < 0:
        abort(400, "err.heizspiegel")
    bands, previous = [], min_m2
    for i, band in enumerate(data["bands"]):
        if not isinstance(band, dict) or not isinstance(band.get("rows"), dict) or not band["rows"] \
                or not set(band["rows"]) <= set(HEIZ_SOURCES):
            abort(400, "err.heizspiegel")
        until = to_number(band.get("until"))
        if (until is None) != (i == len(data["bands"]) - 1) or (until is not None and until <= previous):
            abort(400, "err.heizspiegel")
        previous = until
        rows = {}
        for source, row in band["rows"].items():
            if not isinstance(row, dict):
                abort(400, "err.heizspiegel")
            rows[source] = {"kwh": heiz_limits(row.get("kwh")), "cost": heiz_limits(row.get("cost"))}
        bands.append({"until": until, "rows": rows})
    return {"edition": edition, "min_m2": min_m2, "bands": bands}


def validate_heizspiegels(data):
    """{billing year: edition}, with at least one year."""
    if not isinstance(data, dict) or not data:
        abort(400, "err.heizspiegel")
    if not all(str(a).isdigit() and 2000 <= int(a) <= 2100 for a in data):
        abort(400, "err.benchmark_year")
    return {str(a): validate_heizspiegel(t) for a, t in data.items()}


@app.get("/api/config")
def api_config():
    return jsonify(read_config())


@app.put("/api/config")
def api_save_config():
    data = request.get_json(silent=True) or {}
    u = data.get("place")
    names = None
    if u and isinstance(u, dict) and u.get("id"):  # queried before taking the lock (these are internet requests)
        try:
            names = place_names(u, u["id"])
        except (TypeError, ValueError):
            abort(400, "err.place")
    with lock:
        cfg = read_config()
        if "rooms" in data:
            if not isinstance(data["rooms"], dict):
                abort(400, "err.invalid_data")
            previous = cfg["rooms"]
            cfg["rooms"] = {str(k): str(v).strip()[:40]
                                   for k, v in data["rooms"].items() if str(v).strip()}
            for m in cfg["manual_sensors"]:
                cfg["rooms"].setdefault(m, previous.get(m, "Manual"))
        if "interval_minutes" in data:
            try:
                minutes = int(data["interval_minutes"])
            except (TypeError, ValueError):
                abort(400, "err.interval")
            if not 1 <= minutes <= 1440:
                abort(400, "err.interval")
            change = minutes != cfg["interval_minutes"]
            cfg["interval_minutes"] = minutes
        if "live_seconds" in data:
            seconds = to_number(data["live_seconds"], int)
            if seconds is None or not LIVE_SECONDS_RANGE[0] <= seconds <= LIVE_SECONDS_RANGE[1]:
                abort(400, "err.live_seconds")
            cfg["live_seconds"] = seconds
        for key, (low, high, error) in REF_LIMITS.items():
            if key in data:
                ref = data[key] or {}
                mn, mx = to_number(ref.get("min")), to_number(ref.get("max"))
                if mn is None or mx is None or not low <= mn < mx <= high:
                    abort(400, error)
                cfg[key] = {"min": mn, "max": mx}
        if "water_benchmarks" in data:
            cfg["water_benchmarks"] = validate_benchmarks(data["water_benchmarks"])
        if data.get("restore_benchmark_2024"):
            cfg["water_benchmarks"]["2024"] = json.loads(json.dumps(DEFAULT_CONFIG["water_benchmarks"]["2024"]))
        if "heizspiegels" in data:
            cfg["heizspiegels"] = validate_heizspiegels(data["heizspiegels"])
        if data.get("restore_heizspiegel"):
            year = str(data["restore_heizspiegel"])
            if year not in DEFAULT_CONFIG["heizspiegels"]:
                abort(400, "err.heizspiegel")
            cfg["heizspiegels"][year] = json.loads(json.dumps(DEFAULT_CONFIG["heizspiegels"][year]))
        if "move_in_date" in data:
            v = str(data["move_in_date"] or "")
            if v:
                try:
                    v = datetime.strptime(v, "%Y-%m-%d").date().isoformat()
                except ValueError:
                    abort(400, "err.invalid_date")
            cfg["move_in_date"] = v
        if data.get("place"):
            u = data["place"]
            try:
                lat, lon = float(u["lat"]), float(u["lon"])
            except (KeyError, TypeError, ValueError):
                abort(400, "err.place")
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                abort(400, "err.place")
            cfg["place"] = {"name": str(u.get("name") or f"{lat}, {lon}")[:100], "lat": lat, "lon": lon}
            if names:
                cfg["place"]["names"] = names
            # without names (search without id or no connection) the weather thread fills them in
            with connection() as conn:
                conn.execute("DELETE FROM daily_weather")   # they belonged to the previous place
            global next_archive_fetch
            next_archive_fetch = 0.0
        write_atomic(CONFIG_PATH, json.dumps(cfg, ensure_ascii=False, indent=2))
    # notify the threads only after writing the config, so they read the new value
    if data.get("place"):
        refresh_weather.set()
    if "interval_minutes" in data and change:
        change_interval.set()
    return jsonify(cfg)


@app.get("/api/search_place")
def api_search_place():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    url = GEOCODING + "search?" + urllib.parse.urlencode(
        {"name": q, "count": 8, "language": requested_lang(), "format": "json"})
    try:
        res = fetch_json(url).get("results") or []
    except Exception:  # noqa: BLE001
        abort(502, "err.search")
    return jsonify([{"id": r.get("id"), "name": place_text(r), "lat": r["latitude"], "lon": r["longitude"]}
                    for r in res])


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    migrate_legacy_storage()
    init_db()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"panel-{uuid.uuid4().hex[:8]}")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect_async(MQTT_HOST, MQTT_PORT)
    client.loop_start()
    threading.Thread(target=save_loop, daemon=True).start()
    threading.Thread(target=weather_loop, daemon=True).start()
    print(f"Dashboard on port {PORT}", flush=True)
    # cloudflared adds X-Forwarded-Proto: https; waitress uses it to know the visit came over HTTPS.
    # Only that header is trusted (it solely decides whether the cookie carries the Secure flag).
    serve(app, host="0.0.0.0", port=PORT, threads=8, trusted_proxy="*",
          trusted_proxy_headers={"x-forwarded-proto"}, trusted_proxy_count=1)


if __name__ == "__main__":
    main()
