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
    read_all_from_delta,
    save_data_as_delta,
    extract_opensky_states,
    normalize_states_json,
    clean_states_silver,
    enrich_states_gold,
)

# --- Configuración de rutas del Data Lake ---
DATALAKE_ROOT = "data/etl_datalake"

BRONZE_STATES = f"{DATALAKE_ROOT}/bronze/api_opensky/states"
BRONZE_STATIC = f"{DATALAKE_ROOT}/bronze/api_opensky/aircraft_metadata"

SILVER_STATES = f"{DATALAKE_ROOT}/silver/api_opensky/states"
SILVER_STATIC = f"{DATALAKE_ROOT}/silver/api_opensky/aircraft_metadata"

GOLD_DIR = f"{DATALAKE_ROOT}/gold/api_opensky"
EXPORTS_DIR = f"{DATALAKE_ROOT}/exports"