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
INTERVALO_CLIMA = int(os.environ.get("INTERVALO_CLIMA_SEGUNDOS", "600"))
DATOS = os.environ.get("CARPETA_DATOS", "/datos")
PUERTO = int(os.environ.get("PUERTO", "8000"))

RUTA_CONFIG = os.path.join(DATOS, "config.json")
RUTA_VENTANAS_CSV = os.path.join(DATOS, "ventanas.csv")  # formato viejo, solo para migrar
RUTA_DB = os.path.join(DATOS, "clima.db")
COL_TEMP = ["fecha_hora", "sensor", "temperatura", "humedad", "bateria", "linkquality", "ultima_lectura"]
COL_VENT = ["id", "sensor", "inicio", "fin", "nota"]
AFUERA = "afuera"
# Una lectura más vieja no se guarda: el sensor no está transmitiendo. Igual a availability.passive.timeout de Zigbee2MQTT.
MINUTOS_LECTURA_VALIDA = 120
CONFIG_DEFECTO = {
    "ubicacion": {"nombre": "Wiesbaden, Hessen, Deutschland", "lat": 50.0826, "lon": 8.24,
                  "nombres": {"es": "Wiesbaden, Hessen, Alemania", "en": "Wiesbaden, Hesse, Germany",
                              "de": "Wiesbaden, Hessen, Deutschland"}},
    "habitaciones": {},
    "intervalo_minutos": 5,
    "segundos_vivo": 10,    # cada cuánto consulta el navegador si hay datos nuevos (no afecta lo que se guarda)
    "sensores_manuales": [],   # ids de sensores cuyos registros se cargan a mano (p. ej. "manual_1a2b3c4d")
    # rangos recomendados, se dibujan como referencia en el gráfico de cada magnitud
    "humedad_ref": {"min": 40, "max": 60},
    "temp_ref": {"min": 18, "max": 22},
    "fecha_mudanza": "",    # AAAA-MM-DD: lo consumido antes de esta fecha era del inquilino anterior
    # m2_medidores: superficie por medidor de calefacción; m2_edificio solo se usa si es_departamento
    "vivienda": {"es_departamento": False, "m2_total": None, "m2_edificio": None, "m2_medidores": {}},
}
SEGUNDOS_VIVO = (3, 300)
LIMITES_REF = {"humedad_ref": (0, 100, "err.humedad_ref"), "temp_ref": (-10, 40, "err.temp_ref")}

lock = threading.RLock()
ultimas = {}           # friendly_name -> última lectura
ieee_por_nombre = {}   # friendly_name -> ieee_address
conexion_sensores = {}   # ieee_address -> "online" / "offline" (availability de Zigbee2MQTT)
conexion_pendiente = {}  # friendly_name -> estado recibido antes de conocer su dirección IEEE
version_vivo = 0         # sube con cada dato nuevo; el navegador la consulta para saber cuándo actualizar
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
    client.subscribe(f"{BASE_TOPIC}/+/availability")
    client.subscribe(f"{BASE_TOPIC}/bridge/devices")


def hora_de_lectura(datos):
    """Hora en que Zigbee2MQTT recibió la lectura (last_seen). Sin ella, los mensajes retenidos y los
    republicados parecerían recién llegados aunque el sensor lleve horas sin transmitir."""
    try:
        return datetime.fromisoformat(datos["last_seen"]).replace(tzinfo=None, microsecond=0).isoformat(timespec="seconds")
    except (KeyError, TypeError, ValueError):
        return ahora()


def marcar_cambio():
    global version_vivo
    with lock:
        version_vivo += 1


def on_message(client, userdata, msg):
    try:
        datos = json.loads(msg.payload)
    except ValueError:
        return
    if msg.topic.endswith("/availability"):
        nombre = msg.topic[len(BASE_TOPIC) + 1:-len("/availability")]
        if nombre != "bridge" and isinstance(datos, dict) and datos.get("state") in ("online", "offline"):
            actualizar_conexion(nombre, datos["state"])
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
            pendientes = dict(conexion_pendiente)
            conexion_pendiente.clear()
        for nombre, estado in pendientes.items():
            actualizar_conexion(nombre, estado)
        cerrar_cortes_de_eliminados()
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
            "ultima_lectura": hora_de_lectura(datos),
        }
    marcar_cambio()


def lecturas_actuales():
    """Últimas lecturas indexadas por dirección IEEE (estable aunque se renombre en Zigbee2MQTT)."""
    with lock:
        return {ieee_por_nombre.get(n, n): dict(v) for n, v in ultimas.items()}


def actualizar_conexion(nombre, estado):
    """Abre un corte cuando el sensor pasa a offline y lo cierra cuando vuelve a online.

    Es idempotente: el estado retenido que llega al reiniciar el panel no duplica cortes."""
    with lock:
        ieee = ieee_por_nombre.get(nombre)
        if ieee is None:
            conexion_pendiente[nombre] = estado
            return
        conexion_sensores[ieee] = estado
        lectura = ultimas.get(nombre, {})
    marcar_cambio()
    try:
        with conexion() as con:
            abierto = con.execute("SELECT 1 FROM cortes_sensor WHERE sensor = ? AND hasta IS NULL", (ieee,)).fetchone()
            if estado == "offline" and abierto is None:
                previa = con.execute("SELECT ultima_lectura, linkquality FROM lecturas WHERE sensor = ? "
                                     "AND ultima_lectura IS NOT NULL ORDER BY fecha_hora DESC LIMIT 1", (ieee,)).fetchone()
                ultima = lectura.get("ultima_lectura") or (previa and previa["ultima_lectura"])
                con.execute("INSERT INTO cortes_sensor (id, sensor, desde, ultima_lectura, linkquality) VALUES (?, ?, ?, ?, ?)",
                            (uuid.uuid4().hex[:12], ieee, ahora(), ultima,
                             lectura.get("linkquality") or (previa and previa["linkquality"])))
                # Hasta detectar el corte se fue guardando el último valor con hora nueva: esas copias no son mediciones.
                # La primera fila de esa lectura sí es el dato real y se conserva.
                con.execute("DELETE FROM lecturas WHERE sensor = ? AND ultima_lectura = ? AND fecha_hora > "
                            "(SELECT MIN(fecha_hora) FROM lecturas WHERE sensor = ? AND ultima_lectura = ?)",
                            (ieee, ultima, ieee, ultima))
            elif estado == "online" and abierto is not None:
                con.execute("UPDATE cortes_sensor SET hasta = ? WHERE sensor = ? AND hasta IS NULL", (ahora(), ieee))
    except sqlite3.Error as e:  # un error de la base no debe cortar el hilo de MQTT
        print(f"Error registrando el corte de {nombre}: {e}", flush=True)


def cerrar_cortes_de_eliminados():
    """Un dispositivo eliminado de Zigbee2MQTT ya no enviará 'online': su alerta no debe quedar para siempre."""
    with lock:
        conocidos = list(ieee_por_nombre.values())
    if not conocidos:
        return
    try:
        with conexion() as con:
            con.execute(f"UPDATE cortes_sensor SET hasta = ? WHERE hasta IS NULL AND sensor NOT IN "
                        f"({', '.join('?' * len(conocidos))})", [ahora(), *conocidos])
    except sqlite3.Error as e:
        print(f"Error cerrando cortes: {e}", flush=True)


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

-- Datos de la factura anual de calefacción, por período. Los del edificio solo aplican a un departamento.
CREATE TABLE IF NOT EXISTS facturas_periodo (
    periodo           TEXT PRIMARY KEY,
    kwh_vivienda      REAL,
    unidades_vivienda REAL,
    kwh_edificio      REAL,
    unidades_edificio REAL,
    precio_kwh        REAL,            -- precio de la energía ese año, por kWh
    pagado            REAL             -- monto total pagado al cierre del período, en €
);

-- Cortes de conexión de los sensores (availability de Zigbee2MQTT).
-- 'desde' es cuándo se detectó (tarda lo que indique availability); 'ultima_lectura' es el último dato real.
CREATE TABLE IF NOT EXISTS cortes_sensor (
    id             TEXT PRIMARY KEY,
    sensor         TEXT NOT NULL,
    desde          TEXT NOT NULL,
    hasta          TEXT,           -- NULL mientras el sensor siga sin conexión
    ultima_lectura TEXT,
    linkquality    INTEGER
);
CREATE INDEX IF NOT EXISTS cortes_por_desde ON cortes_sensor (desde);

-- Temperatura media exterior de cada día (Open-Meteo): reparte el consumo entre lecturas según el frío.
-- Se puede volver a descargar; depende de la ubicación configurada y se borra si esta cambia.
CREATE TABLE IF NOT EXISTS clima_diario (
    fecha      TEXT PRIMARY KEY,   -- AAAA-MM-DD
    temp_media REAL NOT NULL
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
    ("facturas_periodo", "precio_kwh", "REAL"),
    ("facturas_periodo", "pagado", "REAL"),
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

def guardar_lecturas(fecha=None):
    fecha = fecha or datetime.now()
    limite = (fecha - timedelta(minutes=MINUTOS_LECTURA_VALIDA)).isoformat(timespec="seconds")
    with lock:
        a = dict(afuera)
        estados = dict(conexion_sensores)
    # Un sensor sin datos recientes queda como hueco, no como una línea plana con su último valor
    filas = [{"fecha_hora": fecha.isoformat(timespec="seconds"), "sensor": s, **v}
             for s, v in sorted(lecturas_actuales().items())
             if v["ultima_lectura"] >= limite and estados.get(s) != "offline"]
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


def leer_al_instante(fecha_iso):
    """Guarda una lectura de todos los sensores con la hora exacta de abrir/cerrar una ventana."""
    try:
        guardar_lecturas(datetime.fromisoformat(fecha_iso))
    except Exception as e:  # noqa: BLE001 - la ventana ya quedó registrada
        print(f"Error guardando la lectura de la ventana: {e}", flush=True)


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
ARCHIVO_CLIMA = "https://archive-api.open-meteo.com/v1/archive?"
HORAS_ENTRE_ARCHIVO = 6
proximo_archivo = 0.0   # time.time() a partir del cual se vuelve a consultar el histórico


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


def completar_clima_diario():
    """Descarga las temperaturas diarias que falten desde el primer período o lectura de medidor hasta ayer."""
    global proximo_archivo
    if time.time() < proximo_archivo:
        return
    with conexion() as con:
        inicio = con.execute("SELECT MIN(f) AS f FROM (SELECT MIN(inicio) AS f FROM periodos "
                             "UNION ALL SELECT MIN(substr(fecha_hora, 1, 10)) FROM lecturas_medidor)").fetchone()["f"]
        conocidas = {r["fecha"] for r in con.execute("SELECT fecha FROM clima_diario")}
    if not inicio:
        return
    primero = datetime.fromisoformat(inicio).date()
    dias = [primero + timedelta(days=i) for i in range((datetime.now().date() - primero).days)]
    faltan = [d.isoformat() for d in dias if d.isoformat() not in conocidas]
    if faltan:
        u = leer_config()["ubicacion"]
        d = obtener_json(ARCHIVO_CLIMA + urllib.parse.urlencode({
            "latitude": u["lat"], "longitude": u["lon"], "timezone": "auto", "daily": "temperature_2m_mean",
            "start_date": faltan[0], "end_date": faltan[-1]}))["daily"]
        filas = [(f, t) for f, t in zip(d["time"], d["temperature_2m_mean"]) if t is not None]
        with conexion() as con:
            con.executemany("INSERT OR REPLACE INTO clima_diario (fecha, temp_media) VALUES (?, ?)", filas)
        print(f"Clima histórico: {len(filas)} días guardados", flush=True)
    proximo_archivo = time.time() + HORAS_ENTRE_ARCHIVO * 3600


def bucle_clima():
    while True:
        try:
            completar_nombres_ubicacion()
        except Exception as e:  # noqa: BLE001
            print(f"Error completando la ubicación: {e}", flush=True)
        try:
            completar_clima_diario()
        except Exception as e:  # noqa: BLE001 - sin histórico el consumo se reparte parejo
            print(f"Error obteniendo el clima histórico: {e}", flush=True)
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


def nivel_termostato(con, sensor, fecha, incluir=True):
    """Nivel vigente del termostato de una habitación en un momento (None si nunca se registró)."""
    r = con.execute(f"SELECT nivel FROM termostatos WHERE sensor = ? AND fecha_hora {'<=' if incluir else '<'} ? "
                    "ORDER BY fecha_hora DESC LIMIT 1", (sensor, fecha)).fetchone()
    return None if r is None else r["nivel"]


def termostato_al_abrir(con, v):
    """Al abrir una ventana, el termostato de la habitación se pone en 0 (si no lo estaba ya)."""
    nivel = nivel_termostato(con, v["sensor"], v["inicio"])
    if nivel:
        con.execute("INSERT OR REPLACE INTO termostatos (id, sensor, fecha_hora, nivel) VALUES (?, ?, ?, 0)",
                    (uuid.uuid4().hex[:12], v["sensor"], v["inicio"]))


def termostato_al_cerrar(con, v):
    """Al cerrar, vuelve al nivel que tenía antes de abrir. Si mientras estuvo abierta alguien
    cambió el termostato a mano (ya no está en 0), se respeta ese cambio."""
    antes = nivel_termostato(con, v["sensor"], v["inicio"], incluir=False)
    if antes and nivel_termostato(con, v["sensor"], v["fin"]) == 0:
        con.execute("INSERT OR REPLACE INTO termostatos (id, sensor, fecha_hora, nivel) VALUES (?, ?, ?, ?)",
                    (uuid.uuid4().hex[:12], v["sensor"], v["fin"], antes))


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


def impacto_ventana(con, v):
    """Cómo cambió la habitación mientras la ventana estuvo abierta (lecturas del mismo sensor).

    Compara la última lectura de hasta 60 min antes de abrir con la mínima de temperatura
    (y la humedad en ese momento) entre la apertura y 30 min después del cierre, porque la habitación
    sigue enfriándose un rato. Si la ventana sigue abierta, hasta ahora.
    """
    fin = (datetime.fromisoformat(v["fin"]) + timedelta(minutes=30)).isoformat(timespec="seconds") if v["fin"] else ahora()
    inicio = datetime.fromisoformat(v["inicio"])
    previo = con.execute(
        "SELECT fecha_hora, temperatura, humedad FROM lecturas WHERE sensor = ? AND fecha_hora <= ? AND fecha_hora >= ? "
        "AND temperatura IS NOT NULL ORDER BY fecha_hora DESC LIMIT 1",
        (v["sensor"], v["inicio"], (inicio - timedelta(minutes=60)).isoformat(timespec="seconds"))).fetchone()
    aproximado = False
    if previo is None:  # sin lectura previa cercana: se usa la más próxima en el tiempo (antes o después), hasta 3 h
        cerca = con.execute(
            "SELECT fecha_hora, temperatura, humedad FROM lecturas WHERE sensor = ? AND fecha_hora BETWEEN ? AND ? "
            "AND temperatura IS NOT NULL", (v["sensor"], (inicio - timedelta(hours=3)).isoformat(timespec="seconds"),
                                            (inicio + timedelta(hours=3)).isoformat(timespec="seconds"))).fetchall()
        if cerca:
            previo = min(cerca, key=lambda r: abs((datetime.fromisoformat(r["fecha_hora"]) - inicio).total_seconds()))
            aproximado = True
    minima = con.execute(
        "SELECT fecha_hora, temperatura, humedad FROM lecturas WHERE sensor = ? AND fecha_hora > ? AND fecha_hora <= ? "
        "AND temperatura IS NOT NULL ORDER BY temperatura ASC, fecha_hora ASC LIMIT 1",
        (v["sensor"], v["inicio"], fin)).fetchone()
    if previo is None or minima is None:
        return None
    # Humedad mínima real (no la del momento más frío). Solo con una lectura previa real y si el sensor reportó durante
    # la apertura o hasta 15 min después del cierre: si estaba caído, lo guardado repite un valor viejo.
    hum_caida = None
    if previo["humedad"] is not None and not aproximado:
        limite = (datetime.fromisoformat(v["fin"]) + timedelta(minutes=15)).isoformat(timespec="seconds") if v["fin"] else ahora()
        medidas = "sensor = ? AND coalesce(ultima_lectura, fecha_hora) >= ? AND coalesce(ultima_lectura, fecha_hora) <= ?"
        reporto = con.execute(f"SELECT 1 FROM lecturas WHERE {medidas} LIMIT 1", (v["sensor"], v["inicio"], limite)).fetchone()
        hum = con.execute(f"SELECT MIN(humedad) AS h FROM lecturas WHERE {medidas} AND humedad IS NOT NULL",
                          (v["sensor"], v["inicio"], fin)).fetchone()["h"]
        if reporto is not None and hum is not None:
            hum_caida = round(previo["humedad"] - hum, 1)
    return {"temp_antes": previo["temperatura"], "temp_min": minima["temperatura"], "hum_caida": hum_caida,
            "caida": round(previo["temperatura"] - minima["temperatura"], 2),
            "hum_antes": previo["humedad"], "hum_min": minima["humedad"],
            "aproximado": aproximado,
            "minutos_a_min": round((datetime.fromisoformat(minima["fecha_hora"]) - datetime.fromisoformat(v["inicio"])).total_seconds() / 60)}


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
    "medidores": {"es": "medidores", "en": "meters", "de": "zaehler"},
    "lecturas_medidor": {"es": "lecturas_medidor", "en": "meter_readings", "de": "zaehlerstaende"},
    "periodos": {"es": "periodos", "en": "periods", "de": "zeitraeume"},
    "cierres_periodo": {"es": "cierres_periodo", "en": "period_closings", "de": "periodenabschluesse"},
    "cortes_sensor": {"es": "cortes_sensor", "en": "sensor_outages", "de": "sensorausfaelle"},
    "facturas_periodo": {"es": "facturas_periodo", "en": "period_bills", "de": "abrechnungen"},
    "todo": {"es": "hausklima_todos_los_datos", "en": "hausklima_all_data", "de": "hausklima_alle_daten"},
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
    "nombre": {"es": "Nombre", "en": "Name", "de": "Name"},
    "valor": {"es": "Valor", "en": "Value", "de": "Wert"},
    "kwh_vivienda": {"es": "kWh vivienda", "en": "kWh home", "de": "kWh Wohnung"},
    "unidades_vivienda": {"es": "Unidades vivienda", "en": "Units home", "de": "Einheiten Wohnung"},
    "kwh_edificio": {"es": "kWh edificio", "en": "kWh building", "de": "kWh Gebäude"},
    "unidades_edificio": {"es": "Unidades edificio", "en": "Units building", "de": "Einheiten Gebäude"},
    "precio_kwh": {"es": "Precio (€/kWh)", "en": "Price (€/kWh)", "de": "Preis (€/kWh)"},
    "pagado": {"es": "Pagado (€)", "en": "Paid (€)", "de": "Bezahlt (€)"},
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


def texto_csv(columnas, filas, idioma):
    buf = io.StringIO()
    buf.write("﻿")  # BOM: así Excel abre bien los acentos y las diéresis
    w = csv.writer(buf)
    w.writerow([CSV_COLUMNAS.get(c, {}).get(idioma, c) for c in columnas])
    w.writerows([[f.get(c, "") for c in columnas] for f in filas])
    return buf.getvalue()


def respuesta_csv(archivo, columnas, filas, idioma):
    buf = io.StringIO(texto_csv(columnas, filas, idioma))
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
    with conexion() as con:
        cortes = {r["sensor"]: dict(r) for r in con.execute("SELECT * FROM cortes_sensor WHERE hasta IS NULL")}
    with lock:
        estados = dict(conexion_sensores)
    ids = set(lect) | set(cfg["habitaciones"]) | manuales
    sensores = [{"id": s, "nombre": nombre_de(s, cfg), "nombre_configurado": cfg["habitaciones"].get(s, ""),
                 "manual": s in manuales, **lect.get(s, {}), "ventana_abierta": abiertas.get(s),
                 "termostato": termos.get(s), "conexion": "offline" if s in cortes else estados.get(s),
                 "corte": cortes.get(s)} for s in ids]
    sensores.sort(key=lambda s: s["nombre"].lower())
    with lock:
        a = dict(afuera)
    return jsonify(sensores=sensores, afuera=a, ubicacion=cfg["ubicacion"],
                   intervalo_minutos=cfg["intervalo_minutos"], segundos_vivo=cfg["segundos_vivo"],
                   humedad_ref=cfg["humedad_ref"], temp_ref=cfg["temp_ref"],
                   fecha_mudanza=cfg["fecha_mudanza"], ahora=ahora())


@app.post("/api/guardar_ahora")
def api_guardar_ahora():
    """Guarda en este momento las lecturas actuales (fuera del intervalo automático)."""
    n = guardar_lecturas()
    if not n:
        abort(409, "err.nada_que_guardar")
    return jsonify(guardadas=n, fecha_hora=ahora())


# ---------- métricas de la casa ----------

MINUTOS_ANTES_APERTURA = 60     # antigüedad máxima de la lectura de referencia antes de abrir
MINUTOS_TRAS_CIERRE = 15        # el sensor debe haber reportado hasta aquí: si no, estaba caído
MINUTOS_RECUPERACION = 60
MINUTOS_HUECO_MAXIMO = 90       # no se interpola entre lecturas más separadas


def serie_real(con, sensor):
    """Lecturas realmente medidas: una por cada 'ultima_lectura', sin las copias que se guardan mientras no hay datos nuevos."""
    vistas, puntos = set(), []
    for r in con.execute("SELECT fecha_hora, temperatura, humedad, ultima_lectura FROM lecturas WHERE sensor = ? "
                         "AND temperatura IS NOT NULL ORDER BY fecha_hora", (sensor,)):
        momento = r["ultima_lectura"] or r["fecha_hora"]
        if momento not in vistas:
            vistas.add(momento)
            puntos.append((datetime.fromisoformat(momento), r["temperatura"], r["humedad"]))
    return sorted(puntos, key=lambda p: p[0])


def interpolar(serie, momento, indice):
    """Valor de la columna 'indice' en un momento, entre las dos lecturas vecinas (None si están muy separadas)."""
    previas = [p for p in serie if p[0] <= momento and p[indice] is not None]
    siguientes = [p for p in serie if p[0] >= momento and p[indice] is not None]
    if not previas or not siguientes:
        return None
    a, b = previas[-1], siguientes[0]
    if a[0] == b[0]:
        return a[indice]
    if b[0] - a[0] > timedelta(minutes=MINUTOS_HUECO_MAXIMO):
        return None
    return a[indice] + (b[indice] - a[indice]) * (momento - a[0]) / (b[0] - a[0])


def efecto_apertura(serie, inicio, fin, siguiente):
    """Humedad y temperatura antes, en el mínimo y una hora después. None si no hubo lecturas reales durante la apertura."""
    previas = [p for p in serie if inicio - timedelta(minutes=MINUTOS_ANTES_APERTURA) <= p[0] <= inicio]
    durante = [p for p in serie if inicio < p[0] <= fin + timedelta(minutes=MINUTOS_TRAS_CIERRE)]
    if not previas or not durante:
        return None
    antes, res = previas[-1], {}
    for clave, indice in (("hum", 2), ("temp", 1)):
        validos = [p for p in durante if p[indice] is not None]
        if antes[indice] is None or not validos:
            continue
        minimo = min(validos, key=lambda p: p[indice])
        minutos = (minimo[0] - inicio).total_seconds() / 60
        res[f"{clave}_caida"] = antes[indice] - minimo[indice]
        if minutos > 0:
            res[f"{clave}_por_min"] = res[f"{clave}_caida"] / minutos
        despues = minimo[0] + timedelta(minutes=MINUTOS_RECUPERACION)
        if despues <= siguiente:   # otra apertura interrumpiría la recuperación
            valor = interpolar(serie, despues, indice)
            if valor is not None:
                res[f"{clave}_recupera"] = valor - minimo[indice]
    return res


def caidas_nocturnas(serie, afuera, ventanas):
    """Baja de temperatura de 23:00 a 06:00, en noches sin ventanas abiertas y con lecturas reales en ambos extremos."""
    res = []
    if not serie:
        return res
    dia = serie[0][0].date()
    while dia < serie[-1][0].date():
        a = datetime.combine(dia, datetime.min.time()) + timedelta(hours=23)
        b = a + timedelta(hours=7)
        dia += timedelta(days=1)
        ta, tb = interpolar(serie, a, 1), interpolar(serie, b, 1)
        if ta is None or tb is None or any(i < b and f > a for i, f in ventanas):
            continue
        exterior = [p[1] for p in afuera if a <= p[0] <= b]
        res.append({"caida": ta - tb, "afuera": statistics.mean(exterior) if exterior else None})
    return res


def mediana(eventos, clave, decimales=1):
    valores = [e[clave] for e in eventos if e.get(clave) is not None]
    return round(statistics.median(valores), decimales) if valores else None


def resumen_metricas(aperturas, noches):
    con_hum = [a for a in aperturas if a.get("hum_caida") is not None]
    con_temp = [a for a in aperturas if a.get("temp_caida") is not None]
    return {"n_hum": len(con_hum), "hum_caida": mediana(aperturas, "hum_caida"), "hum_por_min": mediana(aperturas, "hum_por_min"),
            "hum_recupera": mediana(aperturas, "hum_recupera"), "n_hum_recupera": sum(a.get("hum_recupera") is not None for a in aperturas),
            "n_temp": len(con_temp), "temp_caida": mediana(aperturas, "temp_caida"),
            "temp_recupera": mediana(aperturas, "temp_recupera"), "n_temp_recupera": sum(a.get("temp_recupera") is not None for a in aperturas),
            "n_noches": len(noches), "noche_caida": mediana(noches, "caida"), "noche_afuera": mediana(noches, "afuera")}


@app.get("/api/metricas")
def api_metricas():
    """Cómo se comporta cada habitación, con medianas para que una ducha o una apertura rara no las distorsione."""
    cfg = leer_config()
    manuales = set(cfg["sensores_manuales"])
    todas_aperturas, todas_noches, sensores = [], [], []
    with conexion() as con:
        afuera = serie_real(con, AFUERA)
        ids = [r["sensor"] for r in con.execute("SELECT DISTINCT sensor FROM lecturas") if r["sensor"] != AFUERA and r["sensor"] not in manuales]
        for sensor in ids:
            serie = serie_real(con, sensor)
            ventanas = [(datetime.fromisoformat(r["inicio"]), datetime.fromisoformat(r["fin"])) for r in
                        con.execute("SELECT inicio, fin FROM ventanas WHERE sensor = ? AND fin IS NOT NULL ORDER BY inicio", (sensor,))]
            aperturas = []
            for n, (inicio, fin) in enumerate(ventanas):
                siguiente = ventanas[n + 1][0] if n + 1 < len(ventanas) else datetime.max
                efecto = efecto_apertura(serie, inicio, fin, siguiente)
                if efecto:
                    aperturas.append(efecto)
            noches = caidas_nocturnas(serie, afuera, ventanas)
            todas_aperturas += aperturas
            todas_noches += noches
            sensores.append({"sensor": sensor, "nombre": nombre_de(sensor, cfg), **resumen_metricas(aperturas, noches)})
    sensores.sort(key=lambda s: s["nombre"].lower())
    return jsonify(sensores=sensores, casa=resumen_metricas(todas_aperturas, todas_noches))


# ---------- vivienda y energía ----------

COLUMNAS_FACTURA = ("kwh_vivienda", "unidades_vivienda", "kwh_edificio", "unidades_edificio", "precio_kwh", "pagado")


def medidores_calefaccion(con):
    return con.execute("SELECT id, nombre FROM medidores WHERE tipo = 'calefaccion' ORDER BY nombre COLLATE NOCASE").fetchall()


def positivo(valor):
    """Número mayor que 0, o None si el campo quedó vacío."""
    if valor in ("", None):
        return None
    n = numero(valor)
    if n is None or n <= 0:
        abort(400, "err.vivienda")
    return n


@app.get("/api/vivienda")
def api_vivienda():
    with conexion() as con:
        return jsonify(vivienda=leer_config()["vivienda"], periodos=leer_periodos(con),
                       medidores=[dict(m) for m in medidores_calefaccion(con)],
                       facturas={r["periodo"]: dict(r) for r in con.execute("SELECT * FROM facturas_periodo")})


@app.put("/api/vivienda")
def api_guardar_vivienda():
    datos = request.get_json(silent=True) or {}
    m2_medidores = datos.get("m2_medidores") or {}
    facturas = datos.get("facturas") or {}
    if not isinstance(m2_medidores, dict) or not isinstance(facturas, dict):
        abort(400, "err.datos_invalidos")
    vivienda = {"es_departamento": bool(datos.get("es_departamento")), "m2_total": positivo(datos.get("m2_total")),
                "m2_edificio": positivo(datos.get("m2_edificio")),
                "m2_medidores": {str(k): positivo(v) for k, v in m2_medidores.items() if v not in ("", None)}}
    filas = [(str(p), *(positivo((f or {}).get(c)) for c in COLUMNAS_FACTURA)) for p, f in facturas.items()]
    with lock, conexion() as con:
        conocidos = {p["id"] for p in leer_periodos(con)}
        if any(f[0] not in conocidos for f in filas):
            abort(400, "err.datos_invalidos")
        for f in filas:
            if any(v is not None for v in f[1:]):
                con.execute(f"INSERT OR REPLACE INTO facturas_periodo (periodo, {', '.join(COLUMNAS_FACTURA)}) "
                            f"VALUES ({', '.join('?' * (len(COLUMNAS_FACTURA) + 1))})", f)
            else:
                con.execute("DELETE FROM facturas_periodo WHERE periodo = ?", (f[0],))
        cfg = leer_config()
        cfg["vivienda"] = vivienda
        guardar_config(cfg)
    return jsonify(ok=True)


def unidades_por_periodo(con, medidores, periodos):
    """{período: {medidor: unidades}} contando solo desde la mudanza."""
    res = {p["id"]: {} for p in periodos}
    for m in medidores:
        for f in calcular_consumo(con, m["id"], periodos):
            if f["periodo"] in res and f["consumo"] and not f.get("antes_mudanza"):
                res[f["periodo"]][m["id"]] = res[f["periodo"]].get(m["id"], 0) + f["consumo"]
    return res


def por_m2(kwh, m2, horas):
    if kwh is None or not m2:
        return {"kwh_m2": None, "w_m2": None}
    return {"kwh_m2": kwh / m2, "w_m2": kwh * 1000 / m2 / horas if horas else None}


@app.get("/api/energia")
def api_energia():
    """Energía por período. Las unidades del medidor no son kWh: la factura anual da cuántos kWh vale cada una.
    Sin factura (p. ej. el período en curso) se usa la media ponderada de las facturas anteriores (kWh ÷ unidades
    acumulados) y el resultado queda como estimado. En un departamento se prefieren los totales del edificio: cubren
    años completos, mientras que la vivienda puede tener un año parcial por la mudanza."""
    cfg = leer_config()
    viv = cfg["vivienda"]
    m2_medidores = viv["m2_medidores"]
    with conexion() as con:
        periodos = leer_periodos(con)
        medidores = medidores_calefaccion(con)
        facturas = {r["periodo"]: dict(r) for r in con.execute("SELECT * FROM facturas_periodo")}
        unidades = unidades_por_periodo(con, medidores, periodos)
    es_dpto = viv["es_departamento"]
    m2_vivienda = viv["m2_total"] or sum(v for k, v in m2_medidores.items() if any(m["id"] == k for m in medidores)) or None
    ahora_dt = datetime.now()
    mudanza = datetime.fromisoformat(cfg["fecha_mudanza"]) if cfg["fecha_mudanza"] else None
    filas, ultimo_precio, kwh_acumulado, unidades_acumuladas = [], None, 0.0, 0.0
    for p in periodos:
        f = facturas.get(p["id"], {})
        panel = sum(unidades[p["id"]].values())
        ini, fin = limites_periodo(p)
        if ini > ahora_dt:
            continue
        horas = max((min(fin, ahora_dt) - max(ini, mudanza or ini)).total_seconds() / 3600, 0)
        antes_de_mudarse = bool(mudanza and fin < mudanza)      # no hay consumo propio; sí datos del edificio
        parcial = bool(mudanza and ini < mudanza <= fin)
        base = f.get("unidades_vivienda") or panel
        factor, fuente = None, None
        if f.get("kwh_vivienda") and base:
            factor, fuente = f["kwh_vivienda"] / base, "vivienda"
        elif es_dpto and f.get("kwh_edificio") and f.get("unidades_edificio"):
            factor, fuente = f["kwh_edificio"] / f["unidades_edificio"], "edificio"
        estimado = factor is None
        if estimado:
            factor = kwh_acumulado / unidades_acumuladas if unidades_acumuladas else None
        elif es_dpto and f.get("kwh_edificio") and f.get("unidades_edificio"):
            kwh_acumulado += f["kwh_edificio"]
            unidades_acumuladas += f["unidades_edificio"]
        else:
            kwh_acumulado += factor * base
            unidades_acumuladas += base
        kwh = f["kwh_vivienda"] if fuente == "vivienda" else (panel * factor if factor and not antes_de_mudarse else None)
        precio = f.get("precio_kwh") or ultimo_precio
        ultimo_precio = precio
        fila = {"id": p["id"], "nombre": p["nombre"], "unidades": panel, "unidades_factura": f.get("unidades_vivienda"),
                "factor": factor, "fuente": fuente, "estimado": estimado, "kwh": kwh, **por_m2(kwh, m2_vivienda, horas), "edificio": None,
                "precio": precio, "precio_estimado": bool(precio) and not f.get("precio_kwh"),
                "costo": kwh * precio if kwh is not None and precio else None, "pagado": f.get("pagado")}
        if es_dpto and f.get("kwh_edificio"):
            fila["edificio"] = {"kwh": f["kwh_edificio"], **por_m2(f["kwh_edificio"], viv["m2_edificio"], horas)}
            # Con la mudanza a mitad de período, la vivienda cuenta solo una parte y el edificio el año completo
            if fila["kwh_m2"] and fila["edificio"]["kwh_m2"] and not parcial:
                fila["edificio"]["diferencia"] = (fila["kwh_m2"] / fila["edificio"]["kwh_m2"] - 1) * 100
        filas.append({**fila, "horas": horas, "en_curso": fin >= ahora_dt, "parcial_mudanza": parcial, "por_medidor": {
            m["id"]: {"unidades": unidades[p["id"]].get(m["id"], 0), "kwh": unidades[p["id"]].get(m["id"], 0) * factor if factor else None,
                      **por_m2(unidades[p["id"]].get(m["id"], 0) * factor if factor else None, m2_medidores.get(m["id"]), horas)}
            for m in medidores}})
    return jsonify(es_departamento=es_dpto, m2_vivienda=m2_vivienda, m2_edificio=viv["m2_edificio"], periodos=filas,
                   medidores=[{**dict(m), "m2": m2_medidores.get(m["id"])} for m in medidores])


@app.get("/api/vivo")
def api_vivo():
    """Consulta liviana: el navegador pide /api/estado solo cuando esta versión cambia."""
    return jsonify(version=version_vivo)


@app.get("/api/clima_diario")
def api_clima_diario():
    with conexion() as con:
        return jsonify([[r["fecha"], r["temp_media"]] for r in con.execute("SELECT * FROM clima_diario ORDER BY fecha")])


@app.get("/api/cortes")
def api_cortes():
    """Registro de cortes de conexión de los sensores, del más reciente al más antiguo."""
    cfg = leer_config()
    limite = numero(request.args.get("limite"), int) or 200
    with conexion() as con:
        filas = con.execute("SELECT * FROM cortes_sensor ORDER BY desde DESC LIMIT ?", (min(max(limite, 1), 1000),)).fetchall()
    return jsonify([{**dict(r), "nombre": nombre_de(r["sensor"], cfg)} for r in filas])


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
    with conexion() as con:
        return jsonify([{**con_nombre(v, cfg), "impacto": impacto_ventana(con, v)} for v in ventanas_filtradas()])


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
        if not v["fin"]:
            termostato_al_abrir(con, v)
    if "inicio" not in (request.get_json(silent=True) or {}):
        leer_al_instante(v["inicio"])  # la lectura del momento de abrir es la referencia (máx.)
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
        cerrada_ahora = not v["fin"]
        if cerrada_ahora:
            v["fin"] = max(ahora(), v["inicio"])
            guardar_ventana(con, v)
            termostato_al_cerrar(con, v)
    if cerrada_ahora:
        leer_al_instante(v["fin"])
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
        con.execute("DELETE FROM facturas_periodo WHERE periodo = ?", (id_,))
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


@app.get("/api/todo.zip")
def api_todo_zip():
    """Todas las tablas de la base de datos, sin filtros, en un ZIP con un CSV por tabla."""
    cfg, idioma = leer_config(), idioma_pedido()
    tablas = [("temperaturas", "lecturas"), ("ventanas", "ventanas"), ("termostatos", "termostatos"),
              ("medidores", "medidores"), ("lecturas_medidor", "lecturas_medidor"),
              ("periodos", "periodos"), ("cierres_periodo", "cierres_periodo"), ("cortes_sensor", "cortes_sensor"),
              ("facturas_periodo", "facturas_periodo")]
    orden = {"lecturas": "fecha_hora, sensor", "ventanas": "inicio", "termostatos": "sensor, fecha_hora",
             "lecturas_medidor": "medidor, fecha_hora", "periodos": "inicio", "cortes_sensor": "desde"}
    salida = io.BytesIO()
    with conexion() as con, zipfile.ZipFile(salida, "w", zipfile.ZIP_DEFLATED) as z:
        for archivo, tabla in tablas:
            cur = con.execute(f"SELECT * FROM {tabla}" + (f" ORDER BY {orden[tabla]}" if tabla in orden else ""))
            columnas = [c[0] for c in cur.description]
            filas = [dict(zip(columnas, r)) for r in cur]
            if "sensor" in columnas:
                for f in filas:
                    f["habitacion"] = nombre_de(f["sensor"], cfg, idioma) if f["sensor"] else ""
                columnas.insert(columnas.index("sensor"), "habitacion")
            z.writestr(CSV_ARCHIVOS[archivo].get(idioma, archivo) + ".csv", texto_csv(columnas, filas, idioma))
    return Response(salida.getvalue(), mimetype="application/zip",
                    headers={"Content-Disposition": f'attachment; filename="{CSV_ARCHIVOS["todo"][idioma]}.zip"'})


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


def mover_historial(con, origen, destino):
    """Pasa lecturas, ventanas y termostatos de un sensor a otro. Si ya hay un registro en el
    mismo instante en el destino, queda el del destino."""
    for tabla in ("lecturas", "termostatos"):
        con.execute(f"UPDATE OR IGNORE {tabla} SET sensor = ? WHERE sensor = ?", (destino, origen))
        con.execute(f"DELETE FROM {tabla} WHERE sensor = ?", (origen,))
    con.execute("UPDATE ventanas SET sensor = ? WHERE sensor = ?", (destino, origen))


@app.post("/api/sensores_manuales/<id_>/automatico")
def api_manual_a_automatico(id_):
    """Vincula un sensor manual con un sensor Zigbee: el historial y el nombre pasan al Zigbee."""
    destino = str((request.get_json(silent=True) or {}).get("destino") or "")
    with lock:
        conocidos = set(ieee_por_nombre.values())
    conocidos |= set(lecturas_actuales())  # fuera del lock: lecturas_actuales lo toma
    with lock:
        cfg = leer_config()
        exigir_manual(id_, cfg)
        if (not destino or destino == AFUERA or destino in cfg["sensores_manuales"]
                or destino not in conocidos | set(cfg["habitaciones"])):
            abort(400, "err.destino_invalido")
        with conexion() as con:
            mover_historial(con, id_, destino)
        cfg["habitaciones"][destino] = cfg["habitaciones"].pop(id_, "Manual")
        cfg["sensores_manuales"].remove(id_)
        guardar_config(cfg)
    return jsonify(ok=True, id=destino)


@app.post("/api/sensores_manuales/convertir/<id_>")
def api_automatico_a_manual(id_):
    """Convierte un sensor Zigbee en manual: el historial y el nombre pasan a un sensor manual nuevo."""
    with lock:
        cfg = leer_config()
        if id_ == AFUERA or id_ in cfg["sensores_manuales"]:
            abort(400, "err.no_automatico")
        nuevo = "manual_" + uuid.uuid4().hex[:8]
        with conexion() as con:
            mover_historial(con, id_, nuevo)
        cfg["habitaciones"][nuevo] = cfg["habitaciones"].pop(id_, f"Sensor {id_[-4:]}")
        cfg["sensores_manuales"].append(nuevo)
        guardar_config(cfg)
    return jsonify(ok=True, id=nuevo)


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
        if "segundos_vivo" in datos:
            segundos = numero(datos["segundos_vivo"], int)
            if segundos is None or not SEGUNDOS_VIVO[0] <= segundos <= SEGUNDOS_VIVO[1]:
                abort(400, "err.segundos_vivo")
            cfg["segundos_vivo"] = segundos
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
            with conexion() as con:
                con.execute("DELETE FROM clima_diario")   # eran de la ubicación anterior
            global proximo_archivo
            proximo_archivo = 0.0
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
