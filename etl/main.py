"""
GoodAir – Pipeline ETL
Collecte les données AQICN + OpenWeatherMap toutes les heures
et les insère dans PostgreSQL.
"""

import os
import time
import logging
import requests
import schedule
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# ============================================================
#  CONFIGURATION
# ============================================================
AQICN_TOKEN  = os.getenv("AQICN_TOKEN")
OWM_API_KEY  = os.getenv("OWM_API_KEY")

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

ETL_INTERVAL = int(os.getenv("ETL_INTERVAL_MINUTES", 60))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("goodair_etl")


# ============================================================
#  CONNEXION BASE DE DONNÉES
# ============================================================
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_villes(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, nom, slug_aqicn, latitude, longitude FROM ville WHERE actif = TRUE")
        return cur.fetchall()


# ============================================================
#  COLLECTE AQICN – qualité de l'air
# ============================================================
def fetch_aqicn(slug: str) -> dict | None:
    url = f"https://api.waqi.info/feed/{slug}/?token={AQICN_TOKEN}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("status") != "ok":
            log.warning(f"AQICN {slug}: status={data.get('status')}")
            return None
        return data["data"]
    except Exception as e:
        log.error(f"AQICN {slug}: {e}")
        return None


def insert_aqicn(conn, ville_id: int, data: dict, collecte_le: datetime):
    iaqi = data.get("iaqi", {})
    time_info = data.get("time", {})

    mesure_le = None
    if time_info.get("s"):
        try:
            mesure_le = datetime.strptime(time_info["s"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            pass

    sql = """
        INSERT INTO mesure_air_aqicn (
            ville_id, collecte_le, mesure_le,
            aqi_global, pm25, pm10, no2, o3, co, so2,
            temperature, humidite, pression, vent,
            polluant_dominant, station_nom, station_idx
        ) VALUES (
            %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s
        )
        ON CONFLICT (ville_id, date_trunc('hour', collecte_le)) DO NOTHING
    """

    def v(key):
        return iaqi.get(key, {}).get("v")

    with conn.cursor() as cur:
        cur.execute(sql, (
            ville_id, collecte_le, mesure_le,
            data.get("aqi"),
            v("pm25"), v("pm10"), v("no2"), v("o3"), v("co"), v("so2"),
            v("t"), v("h"), v("p"), v("w"),
            data.get("dominentpol"),
            data.get("city", {}).get("name"),
            data.get("idx"),
        ))

    # Prévisions
    forecast = data.get("forecast", {}).get("daily", {})
    for polluant, jours in forecast.items():
        if polluant not in ("pm25", "pm10", "o3", "uvi"):
            continue
        for jour_data in jours:
            sql_prev = """
                INSERT INTO prevision_air (
                    ville_id, collecte_le, jour, polluant,
                    valeur_avg, valeur_min, valeur_max
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ville_id, jour, polluant, date_trunc('hour', collecte_le)) DO NOTHING
            """
            with conn.cursor() as cur:
                cur.execute(sql_prev, (
                    ville_id, collecte_le,
                    jour_data.get("day"),
                    polluant,
                    jour_data.get("avg"),
                    jour_data.get("min"),
                    jour_data.get("max"),
                ))


# ============================================================
#  COLLECTE OpenWeatherMap – météo
# ============================================================
def fetch_owm_weather(lat: float, lon: float) -> dict | None:
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?lat={lat}&lon={lon}&appid={OWM_API_KEY}&units=metric"
    )
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("cod") != 200:
            log.warning(f"OWM weather lat={lat}: cod={data.get('cod')}")
            return None
        return data
    except Exception as e:
        log.error(f"OWM weather lat={lat}: {e}")
        return None


def insert_meteo(conn, ville_id: int, data: dict, collecte_le: datetime):
    main   = data.get("main", {})
    wind   = data.get("wind", {})
    clouds = data.get("clouds", {})
    weather = data.get("weather", [{}])[0]

    mesure_le = None
    if data.get("dt"):
        mesure_le = datetime.fromtimestamp(data["dt"], tz=timezone.utc).replace(tzinfo=None)

    sql = """
        INSERT INTO mesure_meteo (
            ville_id, collecte_le, mesure_le,
            temperature, temperature_ressentie, temp_min, temp_max,
            pression_hpa, pression_mer_hpa, pression_sol_hpa,
            humidite_pct, vitesse_vent, direction_vent,
            couverture_nuages, visibilite_m,
            condition_code, condition_libelle, condition_desc
        ) VALUES (
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s
        )
        ON CONFLICT (ville_id, date_trunc('hour', collecte_le)) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            ville_id, collecte_le, mesure_le,
            main.get("temp"),
            main.get("feels_like"),
            main.get("temp_min"),
            main.get("temp_max"),
            main.get("pressure"),
            main.get("sea_level"),
            main.get("grnd_level"),
            main.get("humidity"),
            wind.get("speed"),
            wind.get("deg"),
            clouds.get("all"),
            data.get("visibility"),
            weather.get("id"),
            weather.get("main"),
            weather.get("description"),
        ))


# ============================================================
#  COLLECTE OpenWeatherMap – pollution
# ============================================================
def fetch_owm_pollution(lat: float, lon: float) -> dict | None:
    url = (
        f"https://api.openweathermap.org/data/2.5/air_pollution"
        f"?lat={lat}&lon={lon}&appid={OWM_API_KEY}"
    )
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        items = data.get("list", [])
        return items[0] if items else None
    except Exception as e:
        log.error(f"OWM pollution lat={lat}: {e}")
        return None


def insert_pollution(conn, ville_id: int, data: dict, collecte_le: datetime):
    comp = data.get("components", {})

    mesure_le = None
    if data.get("dt"):
        mesure_le = datetime.fromtimestamp(data["dt"], tz=timezone.utc).replace(tzinfo=None)

    sql = """
        INSERT INTO mesure_pollution_owm (
            ville_id, collecte_le, mesure_le, aqi_owm,
            co, no, no2, o3, so2, pm2_5, pm10, nh3
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (ville_id, date_trunc('hour', collecte_le)) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            ville_id, collecte_le, mesure_le,
            data.get("main", {}).get("aqi"),
            comp.get("co"),
            comp.get("no"),
            comp.get("no2"),
            comp.get("o3"),
            comp.get("so2"),
            comp.get("pm2_5"),
            comp.get("pm10"),
            comp.get("nh3"),
        ))


# ============================================================
#  LOG PIPELINE
# ============================================================
def log_pipeline(conn, source: str, statut: str, nb_villes: int,
                 nb_mesures: int, duree: float, erreur: str = None):
    sql = """
        INSERT INTO pipeline_log
            (source, statut, nb_villes, nb_mesures, duree_secondes, message_erreur)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (source, statut, nb_villes, nb_mesures, round(duree, 2), erreur))


# ============================================================
#  PIPELINE PRINCIPAL
# ============================================================
def run_pipeline():
    log.info("=" * 50)
    log.info("Démarrage du pipeline GoodAir")
    collecte_le = datetime.utcnow()

    try:
        conn = get_connection()
    except Exception as e:
        log.error(f"Impossible de se connecter à PostgreSQL : {e}")
        return

    villes = get_villes(conn)
    log.info(f"{len(villes)} villes à traiter")

    # --- AQICN ---
    t0 = time.time()
    nb_ok = 0
    erreurs = []
    for ville_id, nom, slug, lat, lon in villes:
        data = fetch_aqicn(slug)
        if data:
            try:
                insert_aqicn(conn, ville_id, data, collecte_le)
                conn.commit()
                nb_ok += 1
                log.info(f"  AQICN {nom}: AQI={data.get('aqi')} ✓")
            except Exception as e:
                conn.rollback()
                erreurs.append(f"{nom}: {e}")
                log.error(f"  AQICN {nom}: insertion échouée – {e}")
        else:
            erreurs.append(f"{nom}: pas de données")
        time.sleep(0.5)  # respect du quota API

    statut = "SUCCESS" if not erreurs else ("PARTIAL" if nb_ok > 0 else "ERROR")
    log_pipeline(conn, "AQICN", statut, len(villes), nb_ok,
                 time.time() - t0, "; ".join(erreurs) or None)
    conn.commit()
    log.info(f"AQICN terminé : {nb_ok}/{len(villes)} villes – {statut}")

    # --- OWM MÉTÉO ---
    t0 = time.time()
    nb_ok = 0
    erreurs = []
    for ville_id, nom, slug, lat, lon in villes:
        data = fetch_owm_weather(lat, lon)
        if data:
            try:
                insert_meteo(conn, ville_id, data, collecte_le)
                conn.commit()
                nb_ok += 1
                log.info(f"  Météo {nom}: {data['main']['temp']}°C ✓")
            except Exception as e:
                conn.rollback()
                erreurs.append(f"{nom}: {e}")
                log.error(f"  Météo {nom}: {e}")
        time.sleep(0.3)

    statut = "SUCCESS" if not erreurs else ("PARTIAL" if nb_ok > 0 else "ERROR")
    log_pipeline(conn, "OWM_WEATHER", statut, len(villes), nb_ok,
                 time.time() - t0, "; ".join(erreurs) or None)
    conn.commit()
    log.info(f"OWM Météo terminé : {nb_ok}/{len(villes)} villes – {statut}")

    # --- OWM POLLUTION ---
    t0 = time.time()
    nb_ok = 0
    erreurs = []
    for ville_id, nom, slug, lat, lon in villes:
        data = fetch_owm_pollution(lat, lon)
        if data:
            try:
                insert_pollution(conn, ville_id, data, collecte_le)
                conn.commit()
                nb_ok += 1
                log.info(f"  Pollution {nom}: AQI OWM={data['main']['aqi']} ✓")
            except Exception as e:
                conn.rollback()
                erreurs.append(f"{nom}: {e}")
                log.error(f"  Pollution {nom}: {e}")
        time.sleep(0.3)

    statut = "SUCCESS" if not erreurs else ("PARTIAL" if nb_ok > 0 else "ERROR")
    log_pipeline(conn, "OWM_POLLUTION", statut, len(villes), nb_ok,
                 time.time() - t0, "; ".join(erreurs) or None)
    conn.commit()
    log.info(f"OWM Pollution terminé : {nb_ok}/{len(villes)} villes – {statut}")

    conn.close()
    log.info("Pipeline terminé")
    log.info("=" * 50)


# ============================================================
#  SCHEDULER – toutes les heures
# ============================================================
if __name__ == "__main__":
    log.info(f"GoodAir ETL démarré – collecte toutes les {ETL_INTERVAL} minutes")

    # Première exécution immédiate au démarrage
    run_pipeline()

    # Puis toutes les heures
    schedule.every(ETL_INTERVAL).minutes.do(run_pipeline)

    while True:
        schedule.run_pending()
        time.sleep(30)