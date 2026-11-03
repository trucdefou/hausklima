"""Panel de temperaturas.

- Escucha los sensores de Zigbee2MQTT por MQTT.
- Guarda las lecturas (y la temperatura de afuera) en SQLite (datos/clima.db) cada N minutos (configurable).
- Registra aperturas de ventana en la misma base.
- Los CSV se generan al momento de descargarlos.
- Sirve la web del panel.
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
import threading
import time
import urllib.parse
import urllib.request
import uuid
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
INTERVALO_CLIMA = int(os.environ.get("INTERVALO_CLIMA_SEGUNDOS", "600"))
DATOS = os.environ.get("CARPETA_DATOS", "/datos")
PUERTO = int(os.environ.get("PUERTO", "8000"))

RUTA_CONFIG = os.path.join(DATOS, "config.json")
RUTA_VENTANAS_CSV = os.path.join(DATOS, "ventanas.csv")  # formato viejo, solo para migrar
RUTA_DB = os.path.join(DATOS, "clima.db")
COL_TEMP = ["fecha_hora", "sensor", "temperatura", "humedad", "bateria", "linkquality", "ultima_lectura"]
COL_VENT = ["id", "sensor", "inicio", "fin", "nota"]
AFUERA = "afuera"
CONFIG_DEFECTO = {
    "ubicacion": {"nombre": "Wiesbaden, Hessen, Deutschland", "lat": 50.0826, "lon": 8.24,
                  "nombres": {"es": "Wiesbaden, Hessen, Alemania", "en": "Wiesbaden, Hesse, Germany",
                              "de": "Wiesbaden, Hessen, Deutschland"}},
    "habitaciones": {},
    "intervalo_minutos": 5,
    "sensores_manuales": [],   # ids de sensores cuyos registros se cargan a mano (p. ej. "manual_1a2b3c4d")
    # rangos recomendados, se dibujan como referencia en el gráfico de cada magnitud
    "humedad_ref": {"min": 40, "max": 60},
    "temp_ref": {"min": 18, "max": 22},
    "fecha_mudanza": "",    # AAAA-MM-DD: lo consumido antes de esta fecha era del inquilino anterior
}
LIMITES_REF = {"humedad_ref": (0, 100, "err.humedad_ref"), "temp_ref": (-10, 40, "err.temp_ref")}

lock = threading.RLock()
ultimas = {}           # friendly_name -> última lectura
ieee_por_nombre = {}   # friendly_name -> ieee_address
afuera = {}
refrescar_clima = threading.Event()
cambio_intervalo = threading.Event()

CONTRASENA = os.environ.get("PANEL_PASSWORD", "")
if not CONTRASENA:
    raise SystemExit("Falta la variable PANEL_PASSWORD (definila en el archivo .env)")

app = Flask(__name__, static_folder="static")


def clave_secreta():
    """Clave para firmar las cookies de sesión; se genera una vez y se guarda en /datos."""
    ruta = os.path.join(DATOS, ".clave_sesion")
    try:
        with open(ruta, "rb") as f:
            return f.read()
    except FileNotFoundError:
        os.makedirs(DATOS, exist_ok=True)
        clave = secrets.token_bytes(32)
        with open(ruta, "wb") as f:
            f.write(clave)
        os.chmod(ruta, 0o600)
        return clave


class SesionSegunProtocolo(SecureCookieSessionInterface):
    """Cookie 'Secure' cuando se entra por HTTPS (túnel de Cloudflare); sin Secure en la red local por HTTP."""

    def get_cookie_secure(self, app):
        return request.is_secure


app.session_interface = SesionSegunProtocolo()
app.secret_key = clave_secreta()
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=timedelta(days=180),
)
# Si cambia la contraseña, las sesiones abiertas dejan de valer.
HUELLA = hashlib.sha256(app.secret_key + CONTRASENA.encode()).hexdigest()[:16]


# ---------- utilidades ----------

def ahora():
    return datetime.now().isoformat(timespec="seconds")


def normalizar_fecha(valor):
    if not valor:
        return ""
    try:
        return datetime.fromisoformat(str(valor)).replace(tzinfo=None, microsecond=0).isoformat(timespec="seconds")
    except ValueError:
        abort(400, "err.fecha_invalida")


def escribir_atomico(ruta, contenido):
    tmp = ruta + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        f.write(contenido)
    os.replace(tmp, ruta)


def obtener_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "panel-temperaturas/1.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.load(r)


def leer_config():
    try:
        with open(RUTA_CONFIG, encoding="utf-8") as f:
            cfg = json.load(f)
    except (OSError, ValueError):
        cfg = {}
    for clave, valor in CONFIG_DEFECTO.items():
        cfg.setdefault(clave, json.loads(json.dumps(valor)))
    return cfg


NOMBRE_AFUERA = {"es": "Afuera", "en": "Outdoors", "de": "Außen"}
IDIOMAS = ("es", "en", "de")


def nombre_de(sensor, cfg, idioma="es"):
    if sensor == AFUERA:
        return NOMBRE_AFUERA.get(idioma, "Afuera")
    return cfg["habitaciones"].get(sensor) or f"Sensor {sensor[-4:]}"


def idioma_pedido():
    """Idioma de la petición: ?idioma=, cookie 'idioma' o cabecera Accept-Language."""
    for valor in (request.args.get("idioma"), request.cookies.get("idioma")):
        if valor in IDIOMAS:
            return valor
    return request.accept_languages.best_match(IDIOMAS) or "es"


# ---------- MQTT ----------

def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Conectado a MQTT ({reason_code})", flush=True)
    client.subscribe(f"{BASE_TOPIC}/+")
    client.subscribe(f"{BASE_TOPIC}/bridge/devices")


def on_message(client, userdata, msg):
    try:
        datos = json.loads(msg.payload)
    except ValueError:
        return
    if msg.topic == f"{BASE_TOPIC}/bridge/devices":
        if not isinstance(datos, list):
            return
        with lock:
            ieee_por_nombre.clear()
            for d in datos:
                if d.get("type") != "Coordinator":
                    ieee_por_nombre[d["friendly_name"]] = d["ieee_address"]
            for nombre in list(ultimas):
                if nombre not in ieee_por_nombre:
                    del ultimas[nombre]
        return
    nombre = msg.topic[len(BASE_TOPIC) + 1:]
    if nombre == "bridge" or not isinstance(datos, dict) or "temperature" not in datos:
        return
    with lock:
        if ieee_por_nombre and nombre not in ieee_por_nombre:
            return  # mensaje viejo de un dispositivo renombrado/eliminado
        ultimas[nombre] = {
            "temperatura": datos.get("temperature"),
            "humedad": datos.get("humidity"),
            "bateria": datos.get("battery"),
            "linkquality": datos.get("linkquality"),
            "ultima_lectura": ahora(),
        }


def lecturas_actuales():
    """Últimas lecturas indexadas por dirección IEEE (estable aunque se renombre en Zigbee2MQTT)."""
    with lock:
        return {ieee_por_nombre.get(n, n): dict(v) for n, v in ultimas.items()}


# ---------- base de datos (SQLite) ----------

ESQUEMA = """
CREATE TABLE IF NOT EXISTS lecturas (
    fecha_hora     TEXT NOT NULL,
    sensor         TEXT NOT NULL,
    temperatura    REAL,
    humedad        REAL,
    bateria        INTEGER,
    linkquality    INTEGER,
    ultima_lectura TEXT,
    PRIMARY KEY (fecha_hora, sensor)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS lecturas_por_sensor ON lecturas (sensor, fecha_hora);

CREATE TABLE IF NOT EXISTS ventanas (
    id     TEXT PRIMARY KEY,
    sensor TEXT NOT NULL,
    inicio TEXT NOT NULL,
    fin    TEXT,               -- NULL mientras la ventana sigue abierta
    nota   TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS ventanas_por_inicio ON ventanas (inicio);

-- Nivel del termostato de cada habitación: cada fila es un cambio y vale hasta el siguiente
CREATE TABLE IF NOT EXISTS termostatos (
    id         TEXT PRIMARY KEY,
    sensor     TEXT NOT NULL,
    fecha_hora TEXT NOT NULL,
    nivel      REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS termostatos_por_sensor ON termostatos (sensor, fecha_hora);

-- Medidores de calefacción (p. ej. Heizkostenverteiler de cada radiador o el medidor de gas)
CREATE TABLE IF NOT EXISTS medidores (
    id     TEXT PRIMARY KEY,
    nombre TEXT NOT NULL,
    sensor TEXT NOT NULL DEFAULT '',          -- habitación asociada (opcional)
    unidad TEXT NOT NULL DEFAULT ''
);
-- Lecturas del contador; 'reinicio' = el contador volvió a 0 antes de esta lectura (cambio de período)
CREATE TABLE IF NOT EXISTS lecturas_medidor (
    id         TEXT PRIMARY KEY,
    medidor    TEXT NOT NULL,
    fecha_hora TEXT NOT NULL,
    valor      REAL NOT NULL,
    reinicio   INTEGER NOT NULL DEFAULT 0,
    nota       TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS lecturas_medidor_por_fecha ON lecturas_medidor (medidor, fecha_hora);

-- Períodos de liquidación (Abrechnungszeiträume), comunes a toda la casa. Fechas AAAA-MM-DD, 'fin' incluido.
-- Al empezar cada período los contadores vuelven a 0.
CREATE TABLE IF NOT EXISTS periodos (
    id     TEXT PRIMARY KEY,
    nombre TEXT NOT NULL,
    inicio TEXT NOT NULL,
    fin    TEXT NOT NULL
);
-- Valor final de cada medidor en cada período (el Vorjahres-/Stichtagswert que muestra el aparato)
CREATE TABLE IF NOT EXISTS cierres_periodo (
    medidor TEXT NOT NULL,
    periodo TEXT NOT NULL,
    valor   REAL NOT NULL,
    PRIMARY KEY (medidor, periodo)
);
"""
NIVEL_MAX = 5
# 0.5 = ❄ protección antiheladas (en la rueda va entre 0 y 1); de 1 a 5 también valen los medios puntos
NIVELES_VALIDOS = {0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5}


@contextmanager
def conexion():
    """Conexión corta por operación: hace commit al salir bien y rollback si hay error."""
    con = sqlite3.connect(RUTA_DB, timeout=10)
    con.row_factory = sqlite3.Row
    try:
        with con:
            yield con
    finally:
        con.close()


def numero(valor, tipo=float):
    try:
        return tipo(valor) if valor not in ("", None) else None
    except (TypeError, ValueError):
        return None


def migrar_csv():
    """Importa los CSV del formato anterior y los renombra a *.migrado (no se borra nada)."""
    archivos = sorted(glob.glob(os.path.join(DATOS, "temperaturas_*.csv")))
    hay_ventanas = os.path.exists(RUTA_VENTANAS_CSV)
    if not archivos and not hay_ventanas:
        return
    total_l = total_v = 0
    with conexion() as con:
        for ruta in archivos:
            with open(ruta, newline="", encoding="utf-8") as f:
                filas = [(r["fecha_hora"], r["sensor"], numero(r.get("temperatura")), numero(r.get("humedad")),
                          numero(r.get("bateria"), int), numero(r.get("linkquality"), int), r.get("ultima_lectura") or None)
                         for r in csv.DictReader(f) if r.get("fecha_hora", "")[:1].isdigit()]  # salta títulos repetidos
            con.executemany("INSERT OR IGNORE INTO lecturas VALUES (?, ?, ?, ?, ?, ?, ?)", filas)
            total_l += len(filas)
        if hay_ventanas:
            with open(RUTA_VENTANAS_CSV, newline="", encoding="utf-8") as f:
                filas = [(r["id"], r["sensor"], r["inicio"], r.get("fin") or None, r.get("nota") or "")
                         for r in csv.DictReader(f)]
            con.executemany("INSERT OR IGNORE INTO ventanas VALUES (?, ?, ?, ?, ?)", filas)
            total_v = len(filas)
    for ruta in archivos + ([RUTA_VENTANAS_CSV] if hay_ventanas else []):
        os.replace(ruta, ruta + ".migrado")
    print(f"Migración: {total_l} lecturas y {total_v} ventanas importadas a {RUTA_DB}", flush=True)


# Columnas de una versión anterior (inicio de período por medidor y cierre por lectura), reemplazadas por
# las tablas 'periodos' y 'cierres_periodo'. Se mantienen para que las bases existentes sigan siendo válidas.
COLUMNAS_NUEVAS = [
    ("medidores", "inicio_periodo", "TEXT NOT NULL DEFAULT ''"),
    ("lecturas_medidor", "valor_cierre", "REAL"),
    # 'calefaccion' (Heizkostenverteiler: vuelve a 0 en cada período) o 'agua_caliente' (Warmwasserzähler: suma siempre)
    ("medidores", "tipo", "TEXT NOT NULL DEFAULT 'calefaccion'"),
]
TIPOS_MEDIDOR = ("calefaccion", "agua_caliente")


def iniciar_db():
    with conexion() as con:
        con.execute("PRAGMA journal_mode=WAL")  # lecturas y escrituras simultáneas sin bloquearse
        con.executescript(ESQUEMA)
        for tabla, columna, tipo in COLUMNAS_NUEVAS:
            if columna not in {r["name"] for r in con.execute(f"PRAGMA table_info({tabla})")}:
                con.execute(f"ALTER TABLE {tabla} ADD COLUMN {columna} {tipo}")
    migrar_csv()


# ---------- guardado periódico ----------

def guardar_lecturas():
    fecha = datetime.now()
    filas = [{"fecha_hora": fecha.isoformat(timespec="seconds"), "sensor": s, **v}
             for s, v in sorted(lecturas_actuales().items())]
    with lock:
        a = dict(afuera)
    if a.get("temperatura") is not None and \
            datetime.fromisoformat(a["hora"]) > fecha - timedelta(seconds=3 * INTERVALO_CLIMA):
        filas.append({"fecha_hora": fecha.isoformat(timespec="seconds"), "sensor": AFUERA,
                      "temperatura": a["temperatura"], "humedad": a["humedad"], "bateria": None,
                      "linkquality": None, "ultima_lectura": a["hora"]})
    if not filas:
        print("Sin lecturas todavía, no se guarda nada", flush=True)
        return 0
    with conexion() as con:
        con.executemany(f"INSERT OR REPLACE INTO lecturas ({', '.join(COL_TEMP)}) "
                        f"VALUES ({', '.join(':' + c for c in COL_TEMP)})", filas)
    print(f"{len(filas)} lecturas guardadas", flush=True)
    return len(filas)


def bucle_guardado():
    while True:
        intervalo = int(leer_config()["intervalo_minutos"]) * 60
        espera = intervalo - time.time() % intervalo  # alinea a 18:00, 18:05, ...
        if cambio_intervalo.wait(espera):
            cambio_intervalo.clear()
            continue
        try:
            guardar_lecturas()
        except Exception as e:  # noqa: BLE001 - el bucle no debe morir
            print(f"Error guardando: {e}", flush=True)


# ---------- clima de afuera ----------

GEOCODING = "https://geocoding-api.open-meteo.com/v1/"


def texto_ubicacion(r):
    return ", ".join(x for x in (r.get("name"), r.get("admin1"), r.get("country")) if x)


def nombres_ubicacion(u, id_geo=None):
    """Nombre de la ubicación en cada idioma según Open-Meteo, o None si falló la consulta.

    Con el id de Open-Meteo se pide directamente; sin él (configuraciones viejas) se busca por nombre
    y se toma el resultado que coincide con las coordenadas guardadas."""
    nombres = {}
    try:
        for idioma in IDIOMAS:
            if id_geo:
                r = obtener_json(GEOCODING + "get?" + urllib.parse.urlencode({"id": int(id_geo), "language": idioma}))
            else:
                busqueda = urllib.parse.urlencode({"name": u["nombre"].split(",")[0].strip(), "count": 10,
                                                   "language": idioma, "format": "json"})
                r = next((x for x in obtener_json(GEOCODING + "search?" + busqueda).get("results") or []
                          if abs(x["latitude"] - u["lat"]) < 0.1 and abs(x["longitude"] - u["lon"]) < 0.1), None)
            if r and r.get("name"):
                nombres[idioma] = texto_ubicacion(r)[:100]
    except Exception as e:  # noqa: BLE001
        print(f"No se pudieron traducir los nombres de la ubicación: {e}", flush=True)
        return None
    return nombres


def completar_nombres_ubicacion():
    """Agrega los nombres por idioma a una ubicación guardada antes de que existieran."""
    u = leer_config()["ubicacion"]
    if "nombres" in u:
        return
    nombres = nombres_ubicacion(u)
    if nombres is None:
        return  # sin conexión: se reintenta en la próxima vuelta
    with lock:
        cfg = leer_config()
        if cfg["ubicacion"].get("lat") == u["lat"] and cfg["ubicacion"].get("lon") == u["lon"]:
            cfg["ubicacion"]["nombres"] = nombres
            escribir_atomico(RUTA_CONFIG, json.dumps(cfg, ensure_ascii=False, indent=2))

def actualizar_clima():
    u = leer_config()["ubicacion"]
    url = "https://api.open-meteo.com/v1/forecast?" + urllib.parse.urlencode({
        "latitude": u["lat"], "longitude": u["lon"], "timezone": "auto",
        "current": "temperature_2m,relative_humidity_2m,apparent_temperature,weather_code,wind_speed_10m",
    })
    c = obtener_json(url)["current"]
    with lock:
        afuera.clear()
        afuera.update({
            "temperatura": c["temperature_2m"], "humedad": c["relative_humidity_2m"],
            "sensacion": c["apparent_temperature"], "codigo": c["weather_code"],
            "viento": c["wind_speed_10m"], "ubicacion": u["nombre"], "hora": ahora(), "error": None,
        })


def bucle_clima():
    while True:
        try:
            completar_nombres_ubicacion()
        except Exception as e:  # noqa: BLE001
            print(f"Error completando la ubicación: {e}", flush=True)
        try:
            actualizar_clima()
        except Exception as e:  # noqa: BLE001
            print(f"Error obteniendo clima: {e}", flush=True)
            with lock:
                afuera["error"] = str(e)
        refrescar_clima.wait(INTERVALO_CLIMA)
        refrescar_clima.clear()


# ---------- temperaturas ----------

def leer_temperaturas(desde, hasta, sensores):
    cond, params = [], []
    if desde:
        cond.append("fecha_hora >= ?")
        params.append(desde)
    if hasta:
        cond.append("fecha_hora <= ?")
        params.append(hasta)
    if sensores:
        cond.append(f"sensor IN ({', '.join('?' * len(sensores))})")
        params.extend(sorted(sensores))
    sql = f"SELECT {', '.join(COL_TEMP)} FROM lecturas"
    if cond:
        sql += " WHERE " + " AND ".join(cond)
    with conexion() as con:
        return [dict(r) for r in con.execute(sql + " ORDER BY fecha_hora, sensor", params)]


def filtros_temperaturas():
    desde = normalizar_fecha(request.args.get("desde", ""))
    hasta = normalizar_fecha(request.args.get("hasta", ""))
    sensores = {s for s in request.args.get("sensores", "").split(",") if s}
    return desde, hasta, sensores


# ---------- ventanas ----------

def fila_ventana(r):
    return {"id": r["id"], "sensor": r["sensor"], "inicio": r["inicio"], "fin": r["fin"] or "", "nota": r["nota"] or ""}


def obtener_ventana(con, id_):
    r = con.execute("SELECT * FROM ventanas WHERE id = ?", (id_,)).fetchone()
    if r is None:
        abort(404, "err.no_encontrado")
    return fila_ventana(r)


def otra_abierta(con, sensor, excluir=""):
    return con.execute("SELECT 1 FROM ventanas WHERE sensor = ? AND fin IS NULL AND id != ?",
                       (sensor, excluir)).fetchone() is not None


def guardar_ventana(con, v):
    con.execute("INSERT OR REPLACE INTO ventanas (id, sensor, inicio, fin, nota) VALUES (?, ?, ?, ?, ?)",
                (v["id"], v["sensor"], v["inicio"], v["fin"] or None, v["nota"]))


def ultimas_manuales(ids):
    if not ids:
        return {}
    marcas = ", ".join("?" * len(ids))
    sql = f"""SELECT l.sensor, l.temperatura, l.humedad, l.fecha_hora
              FROM lecturas l JOIN (SELECT sensor, MAX(fecha_hora) AS m FROM lecturas
                                    WHERE sensor IN ({marcas}) GROUP BY sensor) u
                ON l.sensor = u.sensor AND l.fecha_hora = u.m"""
    with conexion() as con:
        return {r["sensor"]: {"temperatura": r["temperatura"], "humedad": r["humedad"], "ultima_lectura": r["fecha_hora"]}
                for r in con.execute(sql, list(ids))}


def termostatos_actuales():
    sql = """SELECT t.* FROM termostatos t
             JOIN (SELECT sensor, MAX(fecha_hora) AS m FROM termostatos GROUP BY sensor) u
               ON t.sensor = u.sensor AND t.fecha_hora = u.m"""
    with conexion() as con:
        return {r["sensor"]: {"id": r["id"], "nivel": r["nivel"], "desde": r["fecha_hora"]} for r in con.execute(sql)}


def termostatos_filtrados():
    """Cambios de nivel que estuvieron vigentes en el rango (incluye el que ya regía al empezar)."""
    desde = normalizar_fecha(request.args.get("desde", ""))
    hasta = normalizar_fecha(request.args.get("hasta", ""))
    sensor = request.args.get("sensor", "")
    cond, params = [], []
    if sensor:
        cond.append("sensor = ?")
        params.append(sensor)
    if hasta:
        cond.append("fecha_hora <= ?")
        params.append(hasta)
    if desde:
        cond.append("(hasta IS NULL OR hasta >= ?)")
        params.append(desde)
    sql = ("SELECT * FROM (SELECT id, sensor, fecha_hora, nivel, "
           "LEAD(fecha_hora) OVER (PARTITION BY sensor ORDER BY fecha_hora) AS hasta FROM termostatos)")
    if cond:
        sql += " WHERE " + " AND ".join(cond)
    with conexion() as con:
        return [{**dict(r), "hasta": r["hasta"] or ""} for r in con.execute(sql + " ORDER BY fecha_hora", params)]


def ventanas_abiertas():
    with conexion() as con:
        return {r["sensor"]: fila_ventana(r) for r in con.execute("SELECT * FROM ventanas WHERE fin IS NULL")}


def aplicar_cambios(ventana, datos):
    if "sensor" in datos:
        ventana["sensor"] = str(datos["sensor"] or "")
    if "inicio" in datos:
        ventana["inicio"] = normalizar_fecha(datos["inicio"])
    if "fin" in datos:
        ventana["fin"] = normalizar_fecha(datos["fin"])
    if "nota" in datos:
        ventana["nota"] = str(datos["nota"] or "").strip()[:500]
    if not ventana.get("sensor"):
        abort(400, "err.falta_habitacion")
    if not ventana.get("inicio"):
        abort(400, "err.falta_apertura")
    if ventana.get("fin") and ventana["fin"] < ventana["inicio"]:
        abort(400, "err.fin_antes")
    return ventana


def con_nombre(v, cfg):
    return {**v, "nombre": nombre_de(v["sensor"], cfg)}


def ventanas_filtradas():
    desde = normalizar_fecha(request.args.get("desde", ""))
    hasta = normalizar_fecha(request.args.get("hasta", ""))
    sensor = request.args.get("sensor", "")
    cond, params = [], []
    if sensor:
        cond.append("sensor = ?")
        params.append(sensor)
    if hasta:
        cond.append("inicio <= ?")
        params.append(hasta)
    if desde:
        cond.append("(fin IS NULL OR fin >= ?)")
        params.append(desde)
    sql = "SELECT * FROM ventanas" + (" WHERE " + " AND ".join(cond) if cond else "") + " ORDER BY inicio DESC"
    with conexion() as con:
        return [fila_ventana(r) for r in con.execute(sql, params)]


# Textos de los CSV exportados: nombre de archivo, encabezados y valores fijos, por idioma.
CSV_ARCHIVOS = {
    "temperaturas": {"es": "temperaturas", "en": "temperatures", "de": "temperaturen"},
    "ventanas": {"es": "ventanas", "en": "windows", "de": "fenster"},
    "termostatos": {"es": "termostatos", "en": "thermostats", "de": "thermostate"},
    "consumo": {"es": "consumo", "en": "consumption", "de": "verbrauch"},
}
CSV_COLUMNAS = {
    "fecha_hora": {"es": "Fecha y hora", "en": "Date and time", "de": "Datum und Uhrzeit"},
    "habitacion": {"es": "Habitación", "en": "Room", "de": "Raum"},
    "sensor": {"es": "Sensor", "en": "Sensor", "de": "Sensor"},
    "temperatura": {"es": "Temperatura (°C)", "en": "Temperature (°C)", "de": "Temperatur (°C)"},
    "humedad": {"es": "Humedad (%)", "en": "Humidity (%)", "de": "Luftfeuchtigkeit (%)"},
    "bateria": {"es": "Batería (%)", "en": "Battery (%)", "de": "Batterie (%)"},
    "linkquality": {"es": "Calidad de enlace", "en": "Link quality", "de": "Verbindungsqualität"},
    "ultima_lectura": {"es": "Última lectura del sensor", "en": "Last sensor reading", "de": "Letzte Sensormessung"},
    "inicio": {"es": "Apertura", "en": "Opened", "de": "Geöffnet"},
    "fin": {"es": "Cierre", "en": "Closed", "de": "Geschlossen"},
    "nota": {"es": "Nota", "en": "Note", "de": "Notiz"},
    "id": {"es": "ID", "en": "ID", "de": "ID"},
    "nivel": {"es": "Nivel", "en": "Level", "de": "Stufe"},
    "desde": {"es": "Desde", "en": "From", "de": "Von"},
    "hasta": {"es": "Hasta", "en": "To", "de": "Bis"},
    "medidor": {"es": "Medidor", "en": "Meter", "de": "Zähler"},
    "unidad": {"es": "Unidad", "en": "Unit", "de": "Einheit"},
    "periodo": {"es": "Período", "en": "Period", "de": "Zeitraum"},
    "dias": {"es": "Días", "en": "Days", "de": "Tage"},
    "lectura": {"es": "Lectura", "en": "Reading", "de": "Zählerstand"},
    "consumo": {"es": "Consumo", "en": "Consumption", "de": "Verbrauch"},
    "consumo_por_dia": {"es": "Consumo por día", "en": "Consumption per day", "de": "Verbrauch pro Tag"},
    "acumulado": {"es": "Acumulado", "en": "Cumulative", "de": "Kumuliert"},
    "reinicio": {"es": "Medidor reiniciado", "en": "Meter reset", "de": "Zähler zurückgesetzt"},
    "tipo": {"es": "Tipo", "en": "Type", "de": "Art"},
}
CSV_VALORES = {
    "lectura": {"es": "Lectura", "en": "Reading", "de": "Ablesung"},
    "cierre": {"es": "Cierre de período", "en": "Period close", "de": "Periodenabschluss"},
    "inicio_periodo": {"es": "Inicio de período", "en": "Period start", "de": "Periodenbeginn"},
    "entrada": {"es": "Lectura de entrada", "en": "Move-in reading", "de": "Einzugsablesung"},
    "mudanza": {"es": "Mudanza", "en": "Move-in", "de": "Einzug"},
    "si": {"es": "sí", "en": "yes", "de": "ja"},
    "no": {"es": "no", "en": "no", "de": "nein"},
}


def valor_csv(clave, idioma):
    return CSV_VALORES.get(clave, {}).get(idioma, clave)


def respuesta_csv(archivo, columnas, filas, idioma):
    buf = io.StringIO()
    buf.write("﻿")  # BOM: así Excel abre bien los acentos y las diéresis
    w = csv.writer(buf)
    w.writerow([CSV_COLUMNAS.get(c, {}).get(idioma, c) for c in columnas])
    w.writerows([[f.get(c, "") for c in columnas] for f in filas])
    nombre = CSV_ARCHIVOS[archivo].get(idioma, archivo) + ".csv"
    return Response(buf.getvalue(), mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{nombre}"'})


# ---------- rutas ----------

@app.errorhandler(HTTPException)
def error_json(e):
    return jsonify(error=e.description), e.code


@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


PAGINA_LOGIN = """<!doctype html>
<html lang="__LANG__"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITULO__ · __ENTRAR__</title>
<link rel="icon" type="image/svg+xml" href="/static/favicon.svg">
<link rel="icon" type="image/png" sizes="32x32" href="/static/icono-32.png">
<link rel="apple-touch-icon" sizes="180x180" href="/static/icono-180.png">
<meta name="theme-color" content="#6366f1">
<script>(()=>{let t='auto';try{t=localStorage.getItem('tema')||'auto'}catch(e){}
document.documentElement.setAttribute('data-coreui-theme',t==='auto'?(matchMedia('(prefers-color-scheme: dark)').matches?'dark':'light'):t)})()</script>
<link rel="stylesheet" href="/static/vendor/coreui/coreui.min.css">
<link rel="stylesheet" href="/static/vendor/icons/css/free.min.css">
</head><body>
<div class="bg-body-tertiary min-vh-100 d-flex flex-row align-items-center">
  <div class="container"><div class="row justify-content-center"><div class="col-md-6 col-lg-4">
    <div class="card p-4"><div class="card-body">
      <form method="post" action="/login">
        <h1 class="h3"><i class="cil-home me-2"></i>__TITULO__</h1>
        <p class="text-body-secondary">__SUB__</p>
        <div class="input-group mb-3">
          <span class="input-group-text"><i class="cil-lock-locked"></i></span>
          <input class="form-control" type="password" name="password" placeholder="__PH__" autocomplete="current-password" autofocus required>
        </div>
        __ERROR__
        <button class="btn btn-primary px-4 w-100" type="submit">__ENTRAR__</button>
      </form>
      <div class="text-center small mt-3">__IDIOMAS__</div>
    </div></div>
  </div></div></div>
</div></body></html>"""


TEXTOS_LOGIN = {
    "es": {"TITULO": "Clima de casa", "ENTRAR": "Entrar", "SUB": "Ingresá la contraseña del panel",
           "PH": "Contraseña", "ERROR": "Contraseña incorrecta"},
    "en": {"TITULO": "Home climate", "ENTRAR": "Sign in", "SUB": "Enter the dashboard password",
           "PH": "Password", "ERROR": "Wrong password"},
    "de": {"TITULO": "Raumklima", "ENTRAR": "Anmelden", "SUB": "Passwort für das Dashboard eingeben",
           "PH": "Passwort", "ERROR": "Falsches Passwort"},
}


def pagina_login(error=False):
    idioma = idioma_pedido()
    txt = TEXTOS_LOGIN[idioma]
    html = PAGINA_LOGIN.replace("__ERROR__", f'<div class="alert alert-danger py-2">{txt["ERROR"]}</div>' if error else "")
    html = html.replace("__IDIOMAS__", " · ".join(
        f'<strong>{i.upper()}</strong>' if i == idioma else f'<a href="/login?idioma={i}">{i.upper()}</a>' for i in IDIOMAS))
    html = html.replace("__LANG__", idioma)
    for clave, texto in txt.items():
        html = html.replace(f"__{clave}__", texto)
    resp = Response(html, mimetype="text/html")
    if request.args.get("idioma") in IDIOMAS:
        resp.set_cookie("idioma", idioma, max_age=365 * 24 * 3600, samesite="Lax")
    return resp


def autenticado():
    return session.get("auth") == HUELLA


@app.before_request
def exigir_login():
    if request.path in ("/login", "/logout", "/favicon.ico") or request.path.startswith("/static/") or autenticado():
        return None
    if request.path.startswith("/api/"):
        return jsonify(error="err.sesion"), 401
    return redirect("/login")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if hmac.compare_digest(request.form.get("password", "").encode(), CONTRASENA.encode()):
            session.clear()
            session.permanent = True
            session["auth"] = HUELLA
            return redirect("/")
        time.sleep(1)  # frena intentos por fuerza bruta
        return pagina_login(error=True)
    if autenticado():
        return redirect("/")
    return pagina_login()


@app.get("/favicon.ico")
def favicon():
    return send_from_directory(app.static_folder, "favicon.svg", mimetype="image/svg+xml")


@app.get("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.get("/api/estado")
def api_estado():
    cfg = leer_config()
    manuales = set(cfg["sensores_manuales"])
    lect = {**lecturas_actuales(), **ultimas_manuales(manuales)}
    abiertas = ventanas_abiertas()
    termos = termostatos_actuales()
    ids = set(lect) | set(cfg["habitaciones"]) | manuales
    sensores = [{"id": s, "nombre": nombre_de(s, cfg), "nombre_configurado": cfg["habitaciones"].get(s, ""),
                 "manual": s in manuales, **lect.get(s, {}), "ventana_abierta": abiertas.get(s),
                 "termostato": termos.get(s)} for s in ids]
    sensores.sort(key=lambda s: s["nombre"].lower())
    with lock:
        a = dict(afuera)
    return jsonify(sensores=sensores, afuera=a, ubicacion=cfg["ubicacion"],
                   intervalo_minutos=cfg["intervalo_minutos"], humedad_ref=cfg["humedad_ref"], temp_ref=cfg["temp_ref"],
                   fecha_mudanza=cfg["fecha_mudanza"], ahora=ahora())


@app.post("/api/guardar_ahora")
def api_guardar_ahora():
    """Guarda en este momento las lecturas actuales (fuera del intervalo automático)."""
    n = guardar_lecturas()
    if not n:
        abort(409, "err.nada_que_guardar")
    return jsonify(guardadas=n, fecha_hora=ahora())


@app.get("/api/temperaturas")
def api_temperaturas():
    filas = [[f[c] for c in COL_TEMP] for f in leer_temperaturas(*filtros_temperaturas())]
    return jsonify(columnas=COL_TEMP, filas=filas)


@app.get("/api/temperaturas.csv")
def api_temperaturas_csv():
    cfg = leer_config()
    idioma = idioma_pedido()
    filas = [{**f, "habitacion": nombre_de(f["sensor"], cfg, idioma)} for f in leer_temperaturas(*filtros_temperaturas())]
    return respuesta_csv("temperaturas", ["fecha_hora", "habitacion"] + COL_TEMP[1:], filas, idioma)


@app.get("/api/ventanas")
def api_ventanas():
    cfg = leer_config()
    return jsonify([con_nombre(v, cfg) for v in ventanas_filtradas()])


@app.get("/api/ventanas.csv")
def api_ventanas_csv():
    cfg = leer_config()
    idioma = idioma_pedido()
    filas = [{**v, "habitacion": nombre_de(v["sensor"], cfg, idioma)} for v in ventanas_filtradas()]
    return respuesta_csv("ventanas", ["habitacion", "inicio", "fin", "nota", "sensor", "id"], filas, idioma)


@app.post("/api/ventanas")
def api_crear_ventana():
    datos = request.get_json(silent=True) or {}
    datos.setdefault("inicio", ahora())
    datos.setdefault("fin", "")
    v = aplicar_cambios({"id": uuid.uuid4().hex[:12], "nota": ""}, datos)
    with lock, conexion() as con:
        if not v["fin"] and otra_abierta(con, v["sensor"]):
            abort(409, "err.ya_abierta")
        guardar_ventana(con, v)
    return jsonify(con_nombre(v, leer_config())), 201


@app.put("/api/ventanas/<id_>")
def api_editar_ventana(id_):
    datos = request.get_json(silent=True) or {}
    with lock, conexion() as con:
        v = aplicar_cambios(obtener_ventana(con, id_), datos)
        if not v["fin"] and otra_abierta(con, v["sensor"], excluir=id_):
            abort(409, "err.ya_abierta")
        guardar_ventana(con, v)
    return jsonify(con_nombre(v, leer_config()))


@app.post("/api/ventanas/<id_>/cerrar")
def api_cerrar_ventana(id_):
    with lock, conexion() as con:
        v = obtener_ventana(con, id_)
        if not v["fin"]:
            v["fin"] = max(ahora(), v["inicio"])
            guardar_ventana(con, v)
    return jsonify(con_nombre(v, leer_config()))


@app.delete("/api/ventanas/<id_>")
def api_borrar_ventana(id_):
    with lock, conexion() as con:
        obtener_ventana(con, id_)
        con.execute("DELETE FROM ventanas WHERE id = ?", (id_,))
    return jsonify(ok=True)


# ---------- termostatos ----------

@app.get("/api/termostatos")
def api_termostatos():
    cfg = leer_config()
    return jsonify([{**t, "nombre": nombre_de(t["sensor"], cfg)} for t in termostatos_filtrados()])


@app.get("/api/termostatos.csv")
def api_termostatos_csv():
    cfg = leer_config()
    idioma = idioma_pedido()
    filas = [{**t, "habitacion": nombre_de(t["sensor"], cfg, idioma), "desde": t["fecha_hora"]} for t in termostatos_filtrados()]
    return respuesta_csv("termostatos", ["habitacion", "nivel", "desde", "hasta", "sensor", "id"], filas, idioma)


@app.post("/api/termostatos")
def api_crear_termostato():
    datos = request.get_json(silent=True) or {}
    sensor = str(datos.get("sensor") or "")
    if not sensor:
        abort(400, "err.falta_habitacion")
    nivel = numero(datos.get("nivel"))
    if nivel not in NIVELES_VALIDOS:
        abort(400, "err.nivel_invalido")
    fecha = normalizar_fecha(datos.get("fecha_hora")) or ahora()
    with lock, conexion() as con:
        previo = con.execute("SELECT nivel FROM termostatos WHERE sensor = ? AND fecha_hora <= ? "
                             "ORDER BY fecha_hora DESC LIMIT 1", (sensor, fecha)).fetchone()
        if previo is not None and previo["nivel"] == nivel:
            return jsonify(sin_cambio=True)
        id_ = uuid.uuid4().hex[:12]
        # Un solo cambio por habitación y momento: si ya había uno en esa misma hora, se reemplaza
        con.execute("INSERT OR REPLACE INTO termostatos (id, sensor, fecha_hora, nivel) VALUES (?, ?, ?, ?)",
                    (id_, sensor, fecha, nivel))
    return jsonify(id=id_, sensor=sensor, fecha_hora=fecha, nivel=nivel), 201


@app.put("/api/termostatos/<id_>")
def api_editar_termostato(id_):
    """Corrige el nivel y/o la fecha de un cambio de termostato ya registrado (la habitación no cambia)."""
    datos = request.get_json(silent=True) or {}
    nivel = numero(datos.get("nivel"))
    if nivel not in NIVELES_VALIDOS:
        abort(400, "err.nivel_invalido")
    fecha = normalizar_fecha(datos.get("fecha_hora")) or abort(400, "err.fecha_invalida")
    with lock, conexion() as con:
        fila = con.execute("SELECT sensor FROM termostatos WHERE id = ?", (id_,)).fetchone()
        if fila is None:
            abort(404, "err.no_encontrado")
        if con.execute("SELECT 1 FROM termostatos WHERE sensor = ? AND fecha_hora = ? AND id <> ?",
                       (fila["sensor"], fecha, id_)).fetchone():
            abort(409, "err.termo_duplicado")
        con.execute("UPDATE termostatos SET nivel = ?, fecha_hora = ? WHERE id = ?", (nivel, fecha, id_))
    return jsonify(id=id_, sensor=fila["sensor"], fecha_hora=fecha, nivel=nivel)


@app.delete("/api/termostatos/<id_>")
def api_borrar_termostato(id_):
    with conexion() as con:
        if not con.execute("DELETE FROM termostatos WHERE id = ?", (id_,)).rowcount:
            abort(404, "err.no_encontrado")
    return jsonify(ok=True)


# ---------- medidores de calefacción y consumo ----------

def leer_periodos(con):
    return [dict(r) for r in con.execute("SELECT * FROM periodos ORDER BY inicio")]


def limites_periodo(p):
    """Inicio (00:00 del primer día) y fin (23:59:59 del último día) de un período."""
    return datetime.fromisoformat(p["inicio"]), datetime.fromisoformat(p["fin"]) + timedelta(days=1, seconds=-1)


def periodo_de(periodos, fecha):
    for p in periodos:
        ini, fin = limites_periodo(p)
        if ini <= fecha <= fin:
            return p
    return None


def calcular_consumo(con, medidor, periodos=None):
    """Lecturas en orden con el consumo desde el punto anterior y el acumulado.

    Sin períodos: la primera lectura es la base y un contador que baja se toma como reinicio.
    Con períodos de liquidación se agregan puntos calculados ("virtuales"):
      - el inicio de cada período con el contador en 0 (la lectura cuenta como consumo desde ese día);
      - el cierre de cada período con su valor final, si se cargó.
    Si un período empieza sin conocerse el cierre del anterior, ese tramo queda marcado como sin dato.
    """
    if periodos is None:
        periodos = leer_periodos(con)
    tipo_medidor = con.execute("SELECT tipo FROM medidores WHERE id = ?", (medidor,)).fetchone()
    if tipo_medidor is not None and tipo_medidor["tipo"] != "calefaccion":
        periodos = []      # los contadores de agua no vuelven a 0 al cambiar de período
    cierres = {r["periodo"]: r["valor"] for r in con.execute("SELECT periodo, valor FROM cierres_periodo WHERE medidor = ?", (medidor,))}
    eventos = []   # (fecha, orden en el mismo instante, valor, tipo, fila de lectura, período)
    for r in con.execute("SELECT * FROM lecturas_medidor WHERE medidor = ? ORDER BY fecha_hora", (medidor,)):
        f = datetime.fromisoformat(r["fecha_hora"])
        eventos.append((f, 1, r["valor"], "lectura", r, periodo_de(periodos, f)))
    for p in periodos:
        if p["id"] in cierres:
            eventos.append((limites_periodo(p)[1], 2, cierres[p["id"]], "cierre", None, p))
    if periodos and eventos:
        primero, ultimo = min(e[0] for e in eventos), max(e[0] for e in eventos)
        for p in periodos:
            ini, fin = limites_periodo(p)
            if fin >= primero and ini <= ultimo:      # solo los períodos que tocan los datos de este medidor
                eventos.append((ini, 0, 0.0, "inicio_periodo", None, p))
    eventos.sort(key=lambda e: (e[0], e[1]))

    filas, previa, acumulado = [], None, 0.0
    for fecha, _, valor, tipo, r, p in eventos:
        consumo = dias = None
        reinicio_auto = falta_cierre = False
        if previa is not None:
            dias = (fecha - previa["fecha"]).total_seconds() / 86400
            if tipo == "inicio_periodo":
                consumo = 0.0
                # lo consumido entre el último dato y el fin del período anterior no se conoce
                falta_cierre = previa["tipo"] != "cierre"
            elif r is not None and r["reinicio"]:
                consumo = valor
            elif valor < previa["valor"]:
                consumo, reinicio_auto = valor, True
            else:
                consumo = valor - previa["valor"]
            acumulado += consumo
        filas.append({
            "id": r["id"] if r is not None else None, "medidor": medidor, "tipo": tipo, "virtual": r is None,
            "fecha_hora": fecha.isoformat(timespec="seconds"), "valor": valor,
            "reinicio": bool(r["reinicio"]) if r is not None else False, "reinicio_auto": reinicio_auto,
            "falta_cierre": falta_cierre, "nota": r["nota"] if r is not None else "",
            "periodo": p["id"] if p else "", "periodo_nombre": p["nombre"] if p else "",
            "desde": previa["fecha"].isoformat(timespec="seconds") if previa is not None else "",
            "dias": dias, "consumo": consumo, "acumulado": acumulado,
        })
        previa = {"fecha": fecha, "valor": valor, "tipo": tipo}
    return aplicar_mudanza(filas, leer_config()["fecha_mudanza"])


def aplicar_mudanza(filas, mudanza):
    """Descuenta lo consumido antes de la fecha de mudanza (era del inquilino anterior).

    Si hay una lectura ese mismo día, es la "lectura de entrada" y el cálculo es exacto; si no, el valor
    en el momento de la mudanza se estima repartiendo en partes iguales el tramo entre la lectura anterior y la siguiente.
    """
    if not mudanza or not filas:
        return filas
    m = datetime.fromisoformat(mudanza)
    fechas = [datetime.fromisoformat(f["fecha_hora"]) for f in filas]
    # una lectura real de ese mismo día (a cualquier hora) es la lectura de entrada: el cálculo es exacto
    del_dia = [fe for fe, f in zip(fechas, filas) if not f["virtual"] and fe.date() == m.date()]
    if del_dia:
        m = del_dia[0]
    if m <= fechas[0] and not del_dia:
        return filas                       # todos los datos son posteriores a la mudanza
    exacta = m in fechas
    if exacta:
        base = filas[fechas.index(m)]
        acum_m, valor_m = base["acumulado"], base["valor"]
    elif m >= fechas[-1]:
        acum_m, valor_m = filas[-1]["acumulado"], None
    else:
        i = max(k for k, f in enumerate(fechas) if f < m)
        p, q = filas[i], filas[i + 1]
        frac = (m - fechas[i]) / (fechas[i + 1] - fechas[i])
        acum_m = p["acumulado"] + (q["acumulado"] - p["acumulado"]) * frac
        # el valor del contador solo se puede estimar si no se reinició en ese tramo
        mismo_tramo = q["tipo"] == "lectura" and not q["reinicio"] and not q["reinicio_auto"] or q["tipo"] == "cierre"
        valor_m = p["valor"] + (q["valor"] - p["valor"]) * frac if mismo_tramo else None

    res, ultima = [], None
    for f, fecha in zip(filas, fechas):
        f = dict(f)
        if fecha < m or (exacta and fecha == m):
            if fecha == m:
                f.update(tipo="entrada" if not f["virtual"] else f["tipo"], consumo=None, dias=None)
            else:
                f["antes_mudanza"] = True
            f["acumulado"] = 0.0
            res.append(f)
            ultima = fecha
            continue
        if ultima is not None and ultima < m and not exacta:
            # punto calculado en el momento de la mudanza
            res.append({**f, "id": None, "tipo": "mudanza", "virtual": True, "estimado": True,
                        "fecha_hora": m.isoformat(timespec="seconds"), "valor": valor_m, "consumo": None, "dias": None,
                        "reinicio": False, "reinicio_auto": False, "falta_cierre": False, "nota": "", "desde": "", "acumulado": 0.0})
        if ultima is not None and ultima <= m:     # primer tramo después de la mudanza: cuenta solo desde ese día
            f["desde"] = m.isoformat(timespec="seconds")
            f["dias"] = (fecha - m).total_seconds() / 86400
            f["consumo"] = 0.0 if f["tipo"] == "inicio_periodo" else f["acumulado"] - acum_m
        f["acumulado"] = max(0.0, f["acumulado"] - acum_m)
        res.append(f)
        ultima = fecha
    return res


def leer_medidores(con):
    res = []
    periodos = leer_periodos(con)
    actual = periodo_de(periodos, datetime.now())
    for m in con.execute("SELECT id, nombre, sensor, unidad, tipo FROM medidores ORDER BY nombre COLLATE NOCASE"):
        filas = calcular_consumo(con, m["id"], periodos)
        reales = [f for f in filas if not f["virtual"]]
        ult = reales[-1] if reales else None
        res.append({**dict(m), "lecturas": len(reales), "ultima": ult and {"valor": ult["valor"], "fecha_hora": ult["fecha_hora"]},
                    "acumulado": filas[-1]["acumulado"] if filas else 0,
                    "periodo_actual": actual["nombre"] if actual and m["tipo"] == "calefaccion" else ""})
    return res


def datos_medidor(datos, actual=None):
    m = dict(actual or {"sensor": "", "unidad": "", "tipo": "calefaccion"})
    if "tipo" in datos and actual is None:      # el tipo se elige al crear el medidor
        if datos["tipo"] not in TIPOS_MEDIDOR:
            abort(400, "err.datos_invalidos")
        m["tipo"] = datos["tipo"]
    if "nombre" in datos:
        m["nombre"] = str(datos["nombre"] or "").strip()[:40]
    if "sensor" in datos:
        m["sensor"] = str(datos["sensor"] or "")
    if "unidad" in datos:
        m["unidad"] = str(datos["unidad"] or "").strip()[:15]
    if not m.get("nombre"):
        abort(400, "err.falta_nombre")
    return m


@app.get("/api/medidores")
def api_medidores():
    with conexion() as con:
        return jsonify(leer_medidores(con))


@app.post("/api/medidores")
def api_crear_medidor():
    m = datos_medidor(request.get_json(silent=True) or {})
    m["id"] = "med_" + uuid.uuid4().hex[:8]
    with conexion() as con:
        con.execute("INSERT INTO medidores (id, nombre, sensor, unidad, tipo) VALUES (:id, :nombre, :sensor, :unidad, :tipo)", m)
    return jsonify(m), 201


@app.put("/api/medidores/<id_>")
def api_editar_medidor(id_):
    with conexion() as con:
        actual = con.execute("SELECT * FROM medidores WHERE id = ?", (id_,)).fetchone()
        if actual is None:
            abort(404, "err.no_encontrado")
        m = datos_medidor(request.get_json(silent=True) or {}, dict(actual))
        con.execute("UPDATE medidores SET nombre = :nombre, sensor = :sensor, unidad = :unidad WHERE id = :id", m)
    return jsonify(m)


@app.delete("/api/medidores/<id_>")
def api_borrar_medidor(id_):
    with conexion() as con:
        if not con.execute("DELETE FROM medidores WHERE id = ?", (id_,)).rowcount:
            abort(404, "err.no_encontrado")
        n = con.execute("DELETE FROM lecturas_medidor WHERE medidor = ?", (id_,)).rowcount
        con.execute("DELETE FROM cierres_periodo WHERE medidor = ?", (id_,))
    return jsonify(ok=True, lecturas_borradas=n)


@app.get("/api/lecturas_medidor")
def api_lecturas_medidor():
    """Todas las lecturas con consumo calculado (el filtrado por fechas se hace en la página)."""
    medidor = request.args.get("medidor", "")
    with conexion() as con:
        periodos = leer_periodos(con)
        ids = [medidor] if medidor else [r["id"] for r in con.execute("SELECT id FROM medidores")]
        return jsonify([f for m in ids for f in calcular_consumo(con, m, periodos)])


@app.post("/api/lecturas_medidor")
def api_crear_lectura_medidor():
    datos = request.get_json(silent=True) or {}
    medidor = str(datos.get("medidor") or "")
    valor = numero(datos.get("valor"))
    if valor is None or valor < 0:
        abort(400, "err.lectura_invalida")
    fecha = normalizar_fecha(datos.get("fecha_hora")) or ahora()
    with conexion() as con:
        if con.execute("SELECT 1 FROM medidores WHERE id = ?", (medidor,)).fetchone() is None:
            abort(400, "err.no_encontrado")
        id_ = uuid.uuid4().hex[:12]
        con.execute("INSERT OR REPLACE INTO lecturas_medidor (id, medidor, fecha_hora, valor, reinicio, nota) VALUES (?, ?, ?, ?, ?, ?)",
                    (id_, medidor, fecha, valor, 1 if datos.get("reinicio") else 0, str(datos.get("nota") or "").strip()[:200]))
    return jsonify(id=id_, medidor=medidor, fecha_hora=fecha, valor=valor), 201


@app.put("/api/lecturas_medidor/<id_>")
def api_editar_lectura_medidor(id_):
    datos = request.get_json(silent=True) or {}
    with conexion() as con:
        r = con.execute("SELECT * FROM lecturas_medidor WHERE id = ?", (id_,)).fetchone()
        if r is None:
            abort(404, "err.no_encontrado")
        v = dict(r)
        if "valor" in datos:
            v["valor"] = numero(datos["valor"])
            if v["valor"] is None or v["valor"] < 0:
                abort(400, "err.lectura_invalida")
        if "fecha_hora" in datos:
            v["fecha_hora"] = normalizar_fecha(datos["fecha_hora"]) or abort(400, "err.fecha_invalida")
        if "reinicio" in datos:
            v["reinicio"] = 1 if datos["reinicio"] else 0
        if "nota" in datos:
            v["nota"] = str(datos["nota"] or "").strip()[:200]
        con.execute("UPDATE lecturas_medidor SET fecha_hora = :fecha_hora, valor = :valor, reinicio = :reinicio, "
                    "nota = :nota WHERE id = :id", v)
    return jsonify(v)


@app.delete("/api/lecturas_medidor/<id_>")
def api_borrar_lectura_medidor(id_):
    with conexion() as con:
        if not con.execute("DELETE FROM lecturas_medidor WHERE id = ?", (id_,)).rowcount:
            abort(404, "err.no_encontrado")
    return jsonify(ok=True)


def datos_periodo(datos, actual=None):
    p = dict(actual or {})
    if "nombre" in datos:
        p["nombre"] = str(datos["nombre"] or "").strip()[:40]
    for campo in ("inicio", "fin"):
        if campo in datos:
            try:
                p[campo] = datetime.strptime(str(datos[campo]), "%Y-%m-%d").date().isoformat()
            except ValueError:
                abort(400, "err.fecha_invalida")
    if not p.get("nombre"):
        abort(400, "err.falta_nombre")
    if not p.get("inicio") or not p.get("fin"):
        abort(400, "err.fecha_invalida")
    if p["fin"] < p["inicio"]:
        abort(400, "err.periodo_fechas")
    return p


def validar_superposicion(con, p):
    otro = con.execute("SELECT nombre FROM periodos WHERE id != ? AND inicio <= ? AND fin >= ?",
                       (p.get("id", ""), p["fin"], p["inicio"])).fetchone()
    if otro is not None:
        abort(409, "err.periodo_superpuesto")


@app.get("/api/periodos")
def api_periodos():
    with conexion() as con:
        return jsonify(periodos=leer_periodos(con),
                       cierres=[dict(r) for r in con.execute("SELECT * FROM cierres_periodo")])


@app.post("/api/periodos")
def api_crear_periodo():
    p = datos_periodo(request.get_json(silent=True) or {})
    p["id"] = "per_" + uuid.uuid4().hex[:8]
    with lock, conexion() as con:
        validar_superposicion(con, p)
        con.execute("INSERT INTO periodos (id, nombre, inicio, fin) VALUES (:id, :nombre, :inicio, :fin)", p)
    return jsonify(p), 201


@app.put("/api/periodos/<id_>")
def api_editar_periodo(id_):
    with lock, conexion() as con:
        actual = con.execute("SELECT * FROM periodos WHERE id = ?", (id_,)).fetchone()
        if actual is None:
            abort(404, "err.no_encontrado")
        p = datos_periodo(request.get_json(silent=True) or {}, dict(actual))
        validar_superposicion(con, p)
        con.execute("UPDATE periodos SET nombre = :nombre, inicio = :inicio, fin = :fin WHERE id = :id", p)
    return jsonify(p)


@app.delete("/api/periodos/<id_>")
def api_borrar_periodo(id_):
    with conexion() as con:
        if not con.execute("DELETE FROM periodos WHERE id = ?", (id_,)).rowcount:
            abort(404, "err.no_encontrado")
        con.execute("DELETE FROM cierres_periodo WHERE periodo = ?", (id_,))
    return jsonify(ok=True)


@app.put("/api/periodos/<id_>/cierres")
def api_cierres_periodo(id_):
    """Valores finales del período por medidor: {"valores": {"med_x": 1605, "med_y": ""}} ('' borra)."""
    valores = (request.get_json(silent=True) or {}).get("valores") or {}
    if not isinstance(valores, dict):
        abort(400, "err.datos_invalidos")
    with conexion() as con:
        if con.execute("SELECT 1 FROM periodos WHERE id = ?", (id_,)).fetchone() is None:
            abort(404, "err.no_encontrado")
        for medidor, valor in valores.items():
            if valor in ("", None):
                con.execute("DELETE FROM cierres_periodo WHERE medidor = ? AND periodo = ?", (medidor, id_))
                continue
            v = numero(valor)
            if v is None or v < 0:
                abort(400, "err.lectura_invalida")
            con.execute("INSERT OR REPLACE INTO cierres_periodo (medidor, periodo, valor) VALUES (?, ?, ?)", (medidor, id_, v))
    return jsonify(ok=True)


@app.get("/api/consumo.csv")
def api_consumo_csv():
    desde = normalizar_fecha(request.args.get("desde", ""))
    hasta = normalizar_fecha(request.args.get("hasta", ""))
    medidor = request.args.get("medidor", "")
    tipo = request.args.get("tipo", "")
    cfg, idioma = leer_config(), idioma_pedido()
    filas = []
    with conexion() as con:
        for m in leer_medidores(con):
            if (medidor and m["id"] != medidor) or (tipo and m["tipo"] != tipo):
                continue
            for f in calcular_consumo(con, m["id"]):
                if f["consumo"] is None or f["tipo"] == "inicio_periodo" or f.get("antes_mudanza") or \
                        (desde and f["fecha_hora"] < desde) or (hasta and f["desde"] > hasta):
                    continue
                filas.append({"medidor": m["nombre"], "habitacion": nombre_de(m["sensor"], cfg, idioma) if m["sensor"] else "",
                              "unidad": m["unidad"], "desde": f["desde"], "hasta": f["fecha_hora"],
                              "dias": round(f["dias"], 2), "lectura": f["valor"], "consumo": round(f["consumo"], 3),
                              "consumo_por_dia": round(f["consumo"] / f["dias"], 3) if f["dias"] else "",
                              "acumulado": round(f["acumulado"], 3),
                              "reinicio": valor_csv("si" if f["reinicio"] or f["reinicio_auto"] else "no", idioma),
                              "tipo": valor_csv(f["tipo"], idioma), "periodo": f["periodo_nombre"]})
    return respuesta_csv("consumo", ["medidor", "habitacion", "unidad", "periodo", "desde", "hasta", "dias", "lectura", "consumo",
                                     "consumo_por_dia", "acumulado", "reinicio", "tipo"], filas, idioma)


# ---------- sensores manuales ----------

def guardar_config(cfg):
    escribir_atomico(RUTA_CONFIG, json.dumps(cfg, ensure_ascii=False, indent=2))


def exigir_manual(sensor, cfg):
    if sensor not in cfg["sensores_manuales"]:
        abort(400, "err.no_manual")


@app.post("/api/sensores_manuales")
def api_crear_manual():
    nombre = str((request.get_json(silent=True) or {}).get("nombre") or "").strip()[:40]
    if not nombre:
        abort(400, "err.falta_nombre")
    with lock:
        cfg = leer_config()
        id_ = "manual_" + uuid.uuid4().hex[:8]
        cfg["sensores_manuales"].append(id_)
        cfg["habitaciones"][id_] = nombre
        guardar_config(cfg)
    return jsonify(id=id_, nombre=nombre), 201


@app.delete("/api/sensores_manuales/<id_>")
def api_borrar_manual(id_):
    """Borra el sensor manual junto con sus registros y sus aperturas de ventana."""
    with lock:
        cfg = leer_config()
        exigir_manual(id_, cfg)
        with conexion() as con:
            n = con.execute("DELETE FROM lecturas WHERE sensor = ?", (id_,)).rowcount
            con.execute("DELETE FROM ventanas WHERE sensor = ?", (id_,))
            con.execute("DELETE FROM termostatos WHERE sensor = ?", (id_,))
        cfg["sensores_manuales"].remove(id_)
        cfg["habitaciones"].pop(id_, None)
        guardar_config(cfg)
    return jsonify(ok=True, registros_borrados=n)


@app.post("/api/lecturas_manuales")
def api_crear_lectura_manual():
    datos = request.get_json(silent=True) or {}
    sensor = str(datos.get("sensor") or "")
    exigir_manual(sensor, leer_config())
    fecha = normalizar_fecha(datos.get("fecha_hora")) or abort(400, "err.fecha_invalida")
    temperatura = numero(datos.get("temperatura"))
    humedad = numero(datos.get("humedad"))
    if temperatura is None or not -50 <= temperatura <= 80:
        abort(400, "err.temp_invalida")
    if humedad is not None and not 0 <= humedad <= 100:
        abort(400, "err.humedad_invalida")
    with conexion() as con:
        con.execute("INSERT OR REPLACE INTO lecturas (fecha_hora, sensor, temperatura, humedad, ultima_lectura) "
                    "VALUES (?, ?, ?, ?, ?)", (fecha, sensor, temperatura, humedad, fecha))
    return jsonify(fecha_hora=fecha, sensor=sensor, temperatura=temperatura, humedad=humedad), 201


@app.delete("/api/lecturas_manuales")
def api_borrar_lectura_manual():
    sensor = request.args.get("sensor", "")
    exigir_manual(sensor, leer_config())
    fecha = normalizar_fecha(request.args.get("fecha_hora", ""))
    with conexion() as con:
        n = con.execute("DELETE FROM lecturas WHERE sensor = ? AND fecha_hora = ?", (sensor, fecha)).rowcount
    if not n:
        abort(404, "err.no_encontrado")
    return jsonify(ok=True)


@app.get("/api/config")
def api_config():
    return jsonify(leer_config())


@app.put("/api/config")
def api_guardar_config():
    datos = request.get_json(silent=True) or {}
    u = datos.get("ubicacion")
    nombres = None
    if u and isinstance(u, dict) and u.get("id"):  # se consulta antes de tomar el lock (son pedidos a internet)
        try:
            nombres = nombres_ubicacion(u, u["id"])
        except (TypeError, ValueError):
            abort(400, "err.ubicacion")
    with lock:
        cfg = leer_config()
        if "habitaciones" in datos:
            if not isinstance(datos["habitaciones"], dict):
                abort(400, "err.datos_invalidos")
            anteriores = cfg["habitaciones"]
            cfg["habitaciones"] = {str(k): str(v).strip()[:40]
                                   for k, v in datos["habitaciones"].items() if str(v).strip()}
            for m in cfg["sensores_manuales"]:
                cfg["habitaciones"].setdefault(m, anteriores.get(m, "Manual"))
        if "intervalo_minutos" in datos:
            try:
                minutos = int(datos["intervalo_minutos"])
            except (TypeError, ValueError):
                abort(400, "err.intervalo")
            if not 1 <= minutos <= 1440:
                abort(400, "err.intervalo")
            cambio = minutos != cfg["intervalo_minutos"]
            cfg["intervalo_minutos"] = minutos
        for clave, (bajo, alto, error) in LIMITES_REF.items():
            if clave in datos:
                ref = datos[clave] or {}
                mn, mx = numero(ref.get("min")), numero(ref.get("max"))
                if mn is None or mx is None or not bajo <= mn < mx <= alto:
                    abort(400, error)
                cfg[clave] = {"min": mn, "max": mx}
        if "fecha_mudanza" in datos:
            v = str(datos["fecha_mudanza"] or "")
            if v:
                try:
                    v = datetime.strptime(v, "%Y-%m-%d").date().isoformat()
                except ValueError:
                    abort(400, "err.fecha_invalida")
            cfg["fecha_mudanza"] = v
        if datos.get("ubicacion"):
            u = datos["ubicacion"]
            try:
                lat, lon = float(u["lat"]), float(u["lon"])
            except (KeyError, TypeError, ValueError):
                abort(400, "err.ubicacion")
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                abort(400, "err.ubicacion")
            cfg["ubicacion"] = {"nombre": str(u.get("nombre") or f"{lat}, {lon}")[:100], "lat": lat, "lon": lon}
            if nombres:
                cfg["ubicacion"]["nombres"] = nombres
            # sin nombres (búsqueda sin id o sin conexión) los completa el hilo del clima
        escribir_atomico(RUTA_CONFIG, json.dumps(cfg, ensure_ascii=False, indent=2))
    # avisar a los hilos recién después de escribir la config, para que lean el valor nuevo
    if datos.get("ubicacion"):
        refrescar_clima.set()
    if "intervalo_minutos" in datos and cambio:
        cambio_intervalo.set()
    return jsonify(cfg)


@app.get("/api/buscar_ubicacion")
def api_buscar_ubicacion():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    url = GEOCODING + "search?" + urllib.parse.urlencode(
        {"name": q, "count": 8, "language": idioma_pedido(), "format": "json"})
    try:
        res = obtener_json(url).get("results") or []
    except Exception:  # noqa: BLE001
        abort(502, "err.busqueda")
    return jsonify([{"id": r.get("id"), "nombre": texto_ubicacion(r), "lat": r["latitude"], "lon": r["longitude"]}
                    for r in res])


def main():
    os.makedirs(DATOS, exist_ok=True)
    iniciar_db()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"panel-{uuid.uuid4().hex[:8]}")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect_async(MQTT_HOST, MQTT_PORT)
    client.loop_start()
    threading.Thread(target=bucle_guardado, daemon=True).start()
    threading.Thread(target=bucle_clima, daemon=True).start()
    print(f"Panel en el puerto {PUERTO}", flush=True)
    # cloudflared agrega X-Forwarded-Proto: https; waitress lo usa para saber que la visita fue por HTTPS.
    # Solo se confía en ese encabezado (únicamente decide si la cookie lleva la marca Secure).
    serve(app, host="0.0.0.0", port=PUERTO, threads=8, trusted_proxy="*",
          trusted_proxy_headers={"x-forwarded-proto"}, trusted_proxy_count=1)


if __name__ == "__main__":
    main()
