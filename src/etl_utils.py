# ==========================================================
# etl_utils.py — ETL_OpenSky_Aviation
# ==========================================================

# --- Librerías estándar ---
from datetime import datetime
import os
from pprint import pprint

# --- Librerías externas ---
import requests
import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError


# ==========================================================
# 1. EXTRACCIÓN DE DATOS DESDE OPEN SKY
# ==========================================================

def get_opensky_states(base_url="https://opensky-network.org/api/states/all"):
    """
    Extrae datos en vivo desde la API pública de OpenSky Network.

    Retorna:
    - dict con keys: ["time", "states"]
    - None si ocurre un error
    """
    try:
        response = requests.get(base_url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al obtener datos de OpenSky: {e}")
        return None


# ==========================================================
# 2. TRANSFORMACIÓN DE DATOS
# ==========================================================

def normalize_opensky(json_data):
    """
    Convierte la lista de estados (states) en un DataFrame tabular.

    Documentación oficial: cada elemento de 'states' es una lista con 17 campos.
    """
    if json_data is None or "states" not in json_data:
        return pd.DataFrame()

    states = json_data["states"]
    server_ts = json_data.get("time", None)

    col_names = [
        "icao24", "callsign", "origin_country", "time_position",
        "last_contact", "longitude", "latitude", "baro_altitude",
        "on_ground", "velocity", "true_track", "vertical_rate",
        "sensors", "geo_altitude", "squawk", "spi", "position_source"
    ]

    df = pd.DataFrame(states, columns=col_names)

    # Timestamp del servidor (fecha/hora de generación del snapshot)
    df["server_timestamp"] = pd.to_datetime(server_ts, unit="s")

    return df


def add_extraction_timestamp(df):
    df["extraction_timestamp"] = datetime.utcnow()
    return df


def standardize_columns(df):
    df.columns = (
        df.columns
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(".", "_")
    )
    return df


# ==========================================================
# 3. GUARDADO Y LECTURA EN DELTA LAKE
# ==========================================================

def save_data_as_delta(df, path, mode="overwrite", partition_cols=None):
    """
    Guarda un DataFrame como tabla Delta.
    Crea directorios si no existen.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)

    write_deltalake(
        path,
        df,
        mode=mode,
        partition_by=partition_cols
    )

    print(f"💾 Datos guardados en Delta Lake: {path}")


def save_new_data_as_delta(new_data, data_path, predicate, partition_cols=None):
    """
    Realiza un MERGE que inserta solo nuevos registros según un predicado.
    """
    try:
        dt = DeltaTable(data_path)
        new_pa = pa.Table.from_pandas(new_data)

        dt.merge(
            source=new_pa,
            source_alias="source",
            target_alias="target",
            predicate=predicate
        ).when_not_matched_insert_all().execute()

    except TableNotFoundError:
        save_data_as_delta(new_data, data_path, partition_cols=partition_cols)


def upsert_data_as_delta(data, data_path, predicate):
    """
    MERGE completo:
    - actualiza si existe coincidencia
    - inserta si no existe
    """
    try:
        dt = DeltaTable(data_path)
        data_pa = pa.Table.from_pandas(data)

        dt.merge(
            source=data_pa,
            source_alias="source",
            target_alias="target",
            predicate=predicate
        ).when_matched_update_all() \
         .when_not_matched_insert_all() \
         .execute()

    except TableNotFoundError:
        save_data_as_delta(data, data_path)


# ==========================================================
# 4. LECTURA COMPLETA DESDE DELTA LAKE (para capa Gold)
# ==========================================================

def read_all_from_delta(path):
    """
    Lee todas las particiones de una tabla Delta Lake
    y devuelve un DataFrame de Pandas.

    Uso típico en capa Gold:
    df = read_all_from_delta(silver_dynamic_dir)
    """
    dt = DeltaTable(path)
    return dt.to_pandas()