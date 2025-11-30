# ==========================================================
# etl_opensky_flow.py
# Orquestación del ETL OpenSky con Prefect
# ==========================================================

# -------------------- Importaciones -----------------------

import os
import pandas as pd
from datetime import datetime
from prefect import task, flow

# Módulos propios
from etl_utils import (
    # Extracción
    get_opensky_states,
    clean_static_aircraft_metadata,

    # Transformación dinámica
    normalize_opensky,
    add_extraction_timestamp,
    standardize_columns,
    clean_states_silver,

    # Persistencia
    save_data_as_delta,
    read_all_from_delta,
)


# ==========================================================
# Rutas del Data Lake (relativas a la raíz del proyecto)
# ==========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

DATALAKE_ROOT = os.path.join(BASE_DIR, "data", "etl_datalake")

# Bronze
BRONZE_STATES = os.path.join(DATALAKE_ROOT, "bronze", "api_opensky", "states")
BRONZE_STATIC = os.path.join(DATALAKE_ROOT, "bronze", "api_opensky", "aircraft_metadata")

# Silver
SILVER_STATES = os.path.join(DATALAKE_ROOT, "silver", "api_opensky", "states")
SILVER_STATIC = os.path.join(DATALAKE_ROOT, "silver", "api_opensky", "aircraft_metadata")

# Gold
GOLD_DIR = os.path.join(DATALAKE_ROOT, "gold", "api_opensky")


# ==========================================================
# Tasks — Metadatos estáticos
# ==========================================================

@task(task_run_name="extract-aircraft-metadata")
def task_extract_aircraft_metadata() -> pd.DataFrame:
    """Descarga los metadatos estáticos desde la URL oficial de OpenSky."""
    url = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"
    df = pd.read_csv(url)

    if df.empty:
        raise ValueError("Los metadatos estáticos descargados están vacíos.")

    return df


@task(task_run_name="save-bronze-metadata")
def task_save_bronze_metadata(df_static: pd.DataFrame):
    """Guarda los metadatos estáticos en Bronze (overwrite)."""
    save_data_as_delta(df=df_static, path=BRONZE_STATIC, mode="overwrite")
    return True


@task(task_run_name="process-silver-metadata")
def task_process_silver_metadata(df_static: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa y tipifica los metadatos estáticos para su almacenamiento en la capa Silver.
    Se aplican normalizaciones, conversión segura de fechas y se eliminan columnas
    incompatibles con Delta Lake (p. ej., columnas completamente nulas).
    """
    # Limpieza base definida en etl_utils
    df_silver = clean_static_aircraft_metadata(df_static)

    # Conversión segura de columnas datetime a formato string (requerido por Delta)
    datetime_cols = df_silver.select_dtypes(include=["datetime"]).columns
    for col in datetime_cols:
        df_silver[col] = df_silver[col].dt.strftime("%Y-%m-%d")

    # Eliminación de columnas completamente nulas (Delta Lake no admite NullType)
    df_silver = df_silver.dropna(axis=1, how="all")

    if df_silver.empty:
        raise ValueError("El procesamiento Silver estático devolvió un DataFrame vacío.")

    return df_silver


@task(task_run_name="save-silver-metadata")
def task_save_silver_metadata(df_static_silver: pd.DataFrame):
    """Guarda los metadatos procesados en Silver (overwrite)."""
    save_data_as_delta(df=df_static_silver, path=SILVER_STATIC, mode="overwrite")
    return True


@task(task_run_name="load-silver-metadata")
def task_load_silver_metadata() -> pd.DataFrame:
    """Carga los metadatos estáticos procesados desde Silver."""
    df = read_all_from_delta(SILVER_STATIC)
    if df.empty:
        raise ValueError("No se encontraron metadatos procesados en Silver.")
    return df


# ==========================================================
# Tasks — Snapshot dinámico
# ==========================================================

@task(task_run_name="extract-opensky-states")
def task_extract_states():
    """Extrae el snapshot dinámico del tráfico aéreo (OpenSky)."""
    data = get_opensky_states()
    if data is None:
        raise ValueError("No se pudieron obtener datos desde OpenSky.")
    return data


@task(task_run_name="normalize-opensky-states")
def task_normalize_states(raw_data: dict) -> pd.DataFrame:
    """Normaliza el JSON del snapshot dinámico y agrega timestamps."""
    df = normalize_opensky(raw_data)

    if df.empty:
        raise ValueError("La normalización del snapshot produjo un DataFrame vacío.")

    df = add_extraction_timestamp(df)
    df = standardize_columns(df)
    return df


@task(task_run_name="save-bronze-states")
def task_save_bronze_states(df_normalized: pd.DataFrame):
    """
    Guarda el snapshot dinámico en Bronze aplicando un esquema fijo para
    garantizar consistencia entre ejecuciones manuales y orquestadas.
    """
    df_fixed = df_normalized.convert_dtypes()

    # Eliminación de columnas completamente nulas
    cols_all_null = [c for c in df_fixed.columns if df_fixed[c].isna().all()]
    if cols_all_null:
        df_fixed = df_fixed.drop(columns=cols_all_null)

    # Asegurar tipo string en snapshot_hour si existiera
    if "snapshot_hour" in df_fixed.columns:
        df_fixed["snapshot_hour"] = df_fixed["snapshot_hour"].astype("string")

    # Esquema fijo del recurso "states" de OpenSky
    BRONZE_STATES_SCHEMA = [
        "icao24", "callsign", "origin_country",
        "time_position", "last_contact",
        "longitude", "latitude", "baro_altitude",
        "on_ground", "velocity", "true_track",
        "vertical_rate", "sensors", "geo_altitude",
        "squawk", "spi", "position_source"
    ]

    # Ajuste del DataFrame al esquema fijo
    df_fixed = df_fixed.reindex(columns=BRONZE_STATES_SCHEMA)

    save_data_as_delta(df=df_fixed, path=BRONZE_STATES, mode="append")
    return True


@task(task_run_name="process-silver-states")
def task_process_silver_states(df_bronze: pd.DataFrame) -> pd.DataFrame:
    """Limpieza, tipificación y columnas temporales para Silver dinámico."""
    df_silver = clean_states_silver(df_bronze)

    if df_silver.empty:
        raise ValueError("El procesamiento Silver dinámico devolvió un DataFrame vacío.")

    return df_silver


@task(task_run_name="save-silver-states")
def task_save_silver_states(df_silver: pd.DataFrame):
    """Guarda el snapshot dinámico procesado en Silver (append + particiones)."""

    df_fixed = df_silver.copy()

    # Tipificación segura para Delta Lake
    df_fixed = df_fixed.convert_dtypes()

    # Eliminación de columnas completamente nulas
    cols_all_null = [c for c in df_fixed.columns if df_fixed[c].isna().all()]
    if cols_all_null:
        df_fixed = df_fixed.drop(columns=cols_all_null)

    # snapshot_hour debe ser string
    if "snapshot_hour" in df_fixed.columns:
        df_fixed["snapshot_hour"] = df_fixed["snapshot_hour"].astype("string")

    # Guardado en Delta Lake
    save_data_as_delta(
        df=df_fixed,
        path=SILVER_STATES,
        mode="append",
        partition_cols=["snapshot_hour"]
    )

    return True


@task(task_run_name="load-silver-states")
def task_load_silver_states() -> pd.DataFrame:
    """Carga todas las particiones del snapshot dinámico procesado en Silver."""
    df = read_all_from_delta(SILVER_STATES)
    if df.empty:
        raise ValueError("No se encontraron datos procesados en Silver dinámico.")
    return df


# ==========================================================
# Task — Enriquecimiento y Gold
# ==========================================================

@task(task_run_name="enrich-states-with-metadata")
def task_enrich_states(df_states: pd.DataFrame, df_metadata: pd.DataFrame) -> pd.DataFrame:
    """Enriquece el snapshot dinámico con los metadatos estáticos."""
    df_states["icao24"] = df_states["icao24"].astype("string")
    df_metadata["icao24"] = df_metadata["icao24"].astype("string")

    df_gold = df_states.merge(df_metadata, on="icao24", how="left", suffixes=("", "_meta"))

    if df_gold.empty:
        raise ValueError("El enriquecimiento devolvió un DataFrame vacío.")

    return df_gold


@task(task_run_name="save-gold-states")
def task_save_gold_states(df_gold: pd.DataFrame):
    """Guarda el dataset final enriquecido en Gold (overwrite)."""
    save_data_as_delta(df=df_gold, path=GOLD_DIR, mode="overwrite")
    return True


# ==========================================================
# Flow principal
# ==========================================================

@flow(name="etl-opensky-full-pipeline")
def etl_opensky_flow():
    """
    Pipeline ETL completo del dominio OpenSky:
    - Metadatos estáticos → Bronze → Silver
    - Snapshot dinámico → Bronze → Silver
    - Enriquecimiento final → Gold
    """

    # ----------- Metadatos estáticos -----------
    static_raw = task_extract_aircraft_metadata()
    task_save_bronze_metadata(static_raw)

    static_silver = task_process_silver_metadata(static_raw)
    task_save_silver_metadata(static_silver)

    # ----------- Snapshot dinámico ------------
    raw_states = task_extract_states()
    df_normalized = task_normalize_states(raw_states)
    task_save_bronze_states(df_normalized)

    df_silver = task_process_silver_states(df_normalized)
    task_save_silver_states(df_silver)

    # ----------- Gold -----------
    df_silver_loaded = task_load_silver_states()
    df_metadata_loaded = task_load_silver_metadata()
    df_gold = task_enrich_states(df_silver_loaded, df_metadata_loaded)

    task_save_gold_states(df_gold)

    print("✅ ETL OpenSky ejecutado correctamente.")


# ==========================================================
# Main
# ==========================================================

if __name__ == "__main__":
    print("🚀 Ejecutando ETL_OpenSky (modo manual)...")
    etl_opensky_flow()

    # Scheduling opcional con Prefect (desactivado)
    # etl_opensky_flow.serve(
    #     name="ETL-OpenSky",
    #     cron="0 * * * *"
    # )