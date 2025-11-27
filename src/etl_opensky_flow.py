# ==========================================================
# etl_opensky_flow.py
# Orquestación del ETL OpenSky con Prefect
# ==========================================================


# ==========================================================
# Importaciones
# ==========================================================

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

    # Gold
    read_all_from_delta,

    # Persistencia
    save_data_as_delta,
)


# ==========================================================
# Rutas del Data Lake
# ==========================================================

DATALAKE_ROOT = "data/etl_datalake"

BRONZE_STATES = f"{DATALAKE_ROOT}/bronze/api_opensky/states"
BRONZE_STATIC = f"{DATALAKE_ROOT}/bronze/api_opensky/aircraft_metadata"

SILVER_STATES = f"{DATALAKE_ROOT}/silver/api_opensky/states"
SILVER_STATIC = f"{DATALAKE_ROOT}/silver/api_opensky/aircraft_metadata"

GOLD_DIR = f"{DATALAKE_ROOT}/gold/api_opensky"
EXPORTS_DIR = f"{DATALAKE_ROOT}/exports"


# ==========================================================
# Tasks
# ==========================================================

@task(
    retries=3,
    retry_delay_seconds=30,
    task_run_name="extract-opensky-states"
)
def task_extract_states():
    """
    Task de Prefect que ejecuta la extracción desde la API pública de OpenSky.
    """
    data = get_opensky_states()
    
    if data is None:
        raise ValueError("No se pudieron obtener datos desde OpenSky Network.")
    
    return data


@task(task_run_name="normalize-opensky-states")
def task_normalize_states(raw_data: dict) -> pd.DataFrame:
    """
    Normaliza la estructura JSON del snapshot dinámico.
    """
    df = normalize_opensky(raw_data)

    if df.empty:
        raise ValueError("La normalización del snapshot produjo un DataFrame vacío.")

    df = add_extraction_timestamp(df)
    df = standardize_columns(df)
    return df


@task(task_run_name="save-bronze-states")
def task_save_bronze_states(df_normalized: pd.DataFrame):
    """Guarda el snapshot dinámico en la capa Bronze (append)."""

    # Normalización básica para garantizar compatibilidad con Delta Lake
    df_fixed = df_normalized.convert_dtypes()

    # Delta Lake no acepta columnas completamente nulas → se eliminan
    cols_all_null = [c for c in df_fixed.columns if df_fixed[c].isna().all()]
    if cols_all_null:
        df_fixed = df_fixed.drop(columns=cols_all_null)

    # snapshot_hour (si aparece) debe ser string para particionado estable
    if "snapshot_hour" in df_fixed.columns:
        df_fixed["snapshot_hour"] = df_fixed["snapshot_hour"].astype("string")

    # Persistencia en Bronze (append para mantener historial de snapshots)
    save_data_as_delta(
        df=df_fixed,
        path=BRONZE_STATES,
        mode="append"
    )

    return True


@task(task_run_name="process-silver-states")
def task_process_silver_states(df_bronze: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza, tipificación y creación de columnas temporales para la capa Silver.
    """
    df_silver = clean_states_silver(df_bronze)

    if df_silver.empty:
        raise ValueError("El procesamiento Silver produjo un DataFrame vacío.")

    return df_silver


@task(task_run_name="save-silver-states")
def task_save_silver_states(df_silver: pd.DataFrame):
    """
    Guarda los datos procesados en Silver (append + particiones por hora).
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
    """
    df = read_all_from_delta(SILVER_STATES)

    if df.empty:
        raise ValueError("La lectura desde Silver produjo un DataFrame vacío.")

    return df


@task(task_run_name="load-silver-aircraft-metadata")
def task_load_silver_metadata() -> pd.DataFrame:
    """
    Lee la tabla Silver de metadatos estáticos de aeronaves.
    """
    df = read_all_from_delta(SILVER_STATIC)

    if df.empty:
        raise ValueError("La lectura de metadatos desde Silver produjo un DataFrame vacío.")

    return df


@task(task_run_name="enrich-states-with-metadata")
def task_enrich_states(df_states: pd.DataFrame,
                       df_metadata: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquecimiento del snapshot dinámico con metadatos estáticos (LEFT JOIN por icao24).
    """
    df_states["icao24"] = df_states["icao24"].astype("string")
    df_metadata["icao24"] = df_metadata["icao24"].astype("string")

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
    Guarda el dataset final enriquecido en Gold (overwrite).
    """
    save_data_as_delta(
        df=df_gold,
        path=GOLD_DIR,
        mode="overwrite"
    )
    return True


# ==========================================================
# Flow
# ==========================================================

@flow(name="etl-opensky-full-pipeline")
def etl_opensky_flow():
    """
    Pipeline ETL completo: Bronze → Silver → Gold.
    """
    raw_data = task_extract_states()
    df_normalized = task_normalize_states(raw_data)
    task_save_bronze_states(df_normalized)

    df_silver = task_process_silver_states(df_normalized)
    task_save_silver_states(df_silver)

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

    # Opcional — Scheduling automático (desactivado por defecto)
    #
    # etl_opensky_flow.serve(
    #     name="ETL-OpenSky",
    #     cron="0 * * * *"
    # )