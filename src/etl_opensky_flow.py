# etl_opensky_flow.py
# Script base para la orquestación del ETL OpenSky con Prefect

# --- Librerías estándar ---
import os
from datetime import datetime
from configparser import ConfigParser

# --- Librerías externas ---
import requests
import pandas as pd

# --- Prefect ---
from prefect import task, flow

# --- Módulos propios ---
from etl_utils import (
    # Extracción
    get_opensky_states,

    # Transformación Bronze
    normalize_opensky,
    add_extraction_timestamp,
    standardize_columns,

    # Transformación Silver
    clean_states_silver,

    # Gold (solo lectura)
    read_all_from_delta,

    # Persistencia
    save_data_as_delta,
)


# --- Configuración de rutas del Data Lake ---
DATALAKE_ROOT = "data/etl_datalake"

BRONZE_STATES = f"{DATALAKE_ROOT}/bronze/api_opensky/states"
BRONZE_STATIC = f"{DATALAKE_ROOT}/bronze/api_opensky/aircraft_metadata"

SILVER_STATES = f"{DATALAKE_ROOT}/silver/api_opensky/states"
SILVER_STATIC = f"{DATALAKE_ROOT}/silver/api_opensky/aircraft_metadata"

GOLD_DIR = f"{DATALAKE_ROOT}/gold/api_opensky"
EXPORTS_DIR = f"{DATALAKE_ROOT}/exports"


# -------------------- Tasks --------------------

@task(
    retries=3,
    retry_delay_seconds=30,
    task_run_name="extract-opensky-states"
)
def task_extract_states():
    """
    Task de Prefect que ejecuta la extracción de datos desde la API pública de OpenSky.
    Envuelve la función get_opensky_states() definida en etl_utils.py.
    """
    data = get_opensky_states()
    
    if data is None:
        raise ValueError("No se pudieron obtener datos desde OpenSky Network.")
    
    return data


@task(task_run_name="normalize-opensky-states")
def task_normalize_states(raw_data: dict) -> pd.DataFrame:
    df = normalize_opensky(raw_data)
    
    if df.empty:
        raise ValueError("La normalización del snapshot produjo un DataFrame vacío.")

    df = add_extraction_timestamp(df)
    df = standardize_columns(df)

    return df


@task(task_run_name="save-bronze-states")
def task_save_bronze_states(df_normalized: pd.DataFrame):
    """
    Guarda el snapshot dinámico normalizado en la capa Bronze.
    Se utiliza append para conservar el historial de ingestas.
    """
    save_data_as_delta(
        df=df_normalized,
        path=BRONZE_STATES,
        mode="append"   # Bronze acumula snapshots históricos
    )

    return True


@task(task_run_name="process-silver-states")
def task_process_silver_states(df_bronze: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos del snapshot dinámico para generar la tabla Silver.
    Aplica limpieza, tipificación, columnas temporales y estandarización final.
    """
    df_silver = clean_states_silver(df_bronze)

    if df_silver.empty:
        raise ValueError("El procesamiento Silver produjo un DataFrame vacío.")

    return df_silver


@task(task_run_name="save-silver-states")
def task_save_silver_states(df_silver: pd.DataFrame):
    """
    Guarda los datos procesados en la capa Silver del Data Lake.
    Utiliza append y particiona por 'snapshot_hour' para conservar el historial.
    """
    save_data_as_delta(
        df=df_silver,
        path=SILVER_STATES,
        mode="append",
        partition_cols=["snapshot_hour"]
    )

    return True


@task(task_run_name="load-silver-states")
def task_load_silver_states() -> pd.DataFrame:
    """
    Lee todas las particiones del snapshot dinámico procesado en Silver.
    Devuelve un DataFrame consolidado para su posterior enriquecimiento.
    """
    df_silver_all = read_all_from_delta(SILVER_STATES)

    if df_silver_all.empty:
        raise ValueError("La lectura desde Silver produjo un DataFrame vacío.")

    return df_silver_all


@task(task_run_name="load-silver-aircraft-metadata")
def task_load_silver_metadata() -> pd.DataFrame:
    """
    Lee la tabla de metadatos estáticos procesada en Silver.
    Devuelve un DataFrame con los atributos técnicos de las aeronaves.
    """
    df_metadata = read_all_from_delta(SILVER_STATIC)

    if df_metadata.empty:
        raise ValueError("La lectura de metadatos desde Silver produjo un DataFrame vacío.")

    return df_metadata


@task(task_run_name="enrich-states-with-metadata")
def task_enrich_states(df_states: pd.DataFrame,
                       df_metadata: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquecimiento del snapshot dinámico con los metadatos estáticos de aeronaves.
    Realiza un LEFT JOIN por 'icao24' y devuelve el DataFrame consolidado.
    """

    # Tipificación homogénea del campo clave
    df_states["icao24"] = df_states["icao24"].astype("string")
    df_metadata["icao24"] = df_metadata["icao24"].astype("string")

    # LEFT JOIN → preserva todos los registros del snapshot dinámico
    df_enriched = df_states.merge(
        df_metadata,
        on="icao24",
        how="left",
        suffixes=("", "_meta")
    )

    if df_enriched.empty:
        raise ValueError("El enriquecimiento produjo un DataFrame vacío.")

    return df_enriched


@task(task_run_name="save-gold-states")
def task_save_gold_states(df_gold: pd.DataFrame):
    """
    Guarda el dataset final enriquecido en la capa Gold del Data Lake.
    Se utiliza overwrite para generar siempre la versión más reciente.
    """
    save_data_as_delta(
        df=df_gold,
        path=GOLD_DIR,
        mode="overwrite"
    )

    return True


# -------------------- Flow --------------------

@flow(name="etl-opensky-full-pipeline")
def etl_opensky_flow():
    """
    Pipeline ETL completo para ingestar, procesar y enriquecer datos
    del tráfico aéreo desde OpenSky Network, siguiendo la arquitectura
    Bronze → Silver → Gold.
    """

    # 1) Extracción desde API pública
    raw_data = task_extract_states()

    # 2) Normalización y estandarización (Bronze)
    df_normalized = task_normalize_states(raw_data)

    # 3) Guardado en Bronze (append)
    task_save_bronze_states(df_normalized)

    # 4) Procesamiento Silver (limpieza profunda + columnas temporales)
    df_silver = task_process_silver_states(df_normalized)

    # 5) Guardado en Silver (append + particiones)
    task_save_silver_states(df_silver)

    # 6) Lectura Silver dinámico (para Gold)
    df_silver_loaded = task_load_silver_states()

    # 7) Lectura Silver metadatos estáticos
    df_metadata_loaded = task_load_silver_metadata()

    # 8) Enriquecimiento (LEFT JOIN por icao24)
    df_gold = task_enrich_states(df_silver_loaded, df_metadata_loaded)

    # 9) Guardado final en Gold (overwrite)
    task_save_gold_states(df_gold)

    print("✅ ETL OpenSky ejecutado correctamente.")


# -------------------- Main --------------------

if __name__ == "__main__":
    # Opción A — Ejecución manual (demo o validación local)
    # Ejecuta el flujo una sola vez para probar la orquestación de OpenSky.
    print("🚀 Ejecutando ETL_OpenSky (modo manual)...")
    etl_opensky_flow()

    # Opción B — Ejecución automatizada (opcional, DESACTIVADA por defecto)
    # Sirve el flujo como servicio local con programación horaria.
    # ADVERTENCIA: No activar si estás ejecutando en entorno local,
    # ya que generará extractos nuevos cada hora y puede crecer rápidamente el tamaño del Data Lake.
    #
    # etl_opensky_flow.serve(
    #     name="ETL-OpenSky",
    #     cron="0 * * * *"  # Cada hora en punto (ver https://crontab.guru para modificar)
    # )