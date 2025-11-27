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


def clean_states_silver(df):
    """
    Limpieza y estandarización del snapshot dinámico para la capa Silver.

    Incluye:
    - Conversión de timestamps y creación de columnas derivadas (`snapshot_time`, `snapshot_hour`)
    - Tipificación numérica segura (coordenadas, altitudes, velocidad, rumbo)
    - Conversión de columnas booleanas
    - Tipificación categórica para claves aeronáuticas
    - Eliminación de columnas sin relevancia analítica

    Retorna un DataFrame limpio, tipado y consistente, listo para su uso en la capa Gold.
    """

    df = df.copy()

    # Conversión de timestamps
    if "server_timestamp" in df.columns:
        df["server_timestamp"] = pd.to_datetime(df["server_timestamp"], errors="coerce")

    if "extraction_timestamp" in df.columns:
        df["extraction_timestamp"] = pd.to_datetime(df["extraction_timestamp"], errors="coerce")

    df["snapshot_time"] = df["server_timestamp"].fillna(df["extraction_timestamp"])
    df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], errors="coerce")
    df["snapshot_hour"] = df["snapshot_time"].dt.strftime("%Y-%m-%d-%H")

    # Tipificación numérica
    numeric_cols = [
        "longitude", "latitude", "baro_altitude", "geo_altitude",
        "velocity", "true_track", "vertical_rate"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Tipificación booleana
    bool_cols = ["on_ground", "spi"]
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].astype("boolean")

    # Tipificación categórica
    if "icao24" in df.columns:
        df["icao24"] = df["icao24"].astype("string")

    # Eliminación de columnas irrelevantes
    cols_drop = ["sensors"]
    df = df.drop(columns=[c for c in cols_drop if c in df.columns], errors="ignore")

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


# ==========================================================
# 5. TRANSFORMACIONES GOLD (enriquecimiento y dataset final)
# ==========================================================

def prepare_states_for_gold(df_states_silver):
    """
    Prepara los datos del snapshot dinámico (Silver) para la capa Gold.
    
    Este paso incluye:
    - estandarización de tipos (string para columnas categóricas)
    - conversión de timestamps si fuera necesario
    - limpieza de columnas innecesarias previas al enriquecimiento
    
    Parámetros
    ----------
    df_states_silver : pd.DataFrame
        DataFrame proveniente de la capa Silver, ya normalizado.
    
    Retorna
    -------
    pd.DataFrame
        DataFrame listo para ser enriquecido con metadatos estáticos.
    """

    df = df_states_silver.copy()

    # Columnas categóricas que se tipifican como string
    cols_string = ["icao24", "callsign", "origin_country", "squawk"]
    for col in cols_string:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # Conversión segura de columnas temporales a datetime
    if "snapshot_time" in df.columns:
        df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], errors="coerce")

    return df


def enrich_states_with_metadata(df_states, df_aircraft):
    """
    Enriquecimiento del snapshot dinámico mediante JOIN con la tabla 
    de metadatos estáticos de aeronaves.
    
    El cruce se realiza por la clave primaria 'icao24'.
    
    Parámetros
    ----------
    df_states : pd.DataFrame
        DataFrame con el snapshot dinámico ya preparado para Gold.
    
    df_aircraft : pd.DataFrame
        DataFrame proveniente de Silver con metadatos estáticos limpios.
    
    Retorna
    -------
    pd.DataFrame
        DataFrame enriquecido que combina información operativa (dinámica)
        con atributos técnicos del avión (estática).
    """

    df_states = df_states.copy()
    df_aircraft = df_aircraft.copy()

    # Asegurar tipo string en la clave de join
    df_states["icao24"] = df_states["icao24"].astype("string")
    df_aircraft["icao24"] = df_aircraft["icao24"].astype("string")

    # Join tipo LEFT → se preservan todos los registros dinámicos
    df_enriched = df_states.merge(
        df_aircraft,
        on="icao24",
        how="left",
        suffixes=("", "_meta")
    )

    return df_enriched


def build_gold_final_dataset(df_enriched):
    """
    Construye el dataset final de la capa Gold.
    
    Selecciona y ordena las columnas claves para análisis, combinando:
    - información operativa del snapshot dinámico
    - atributos técnicos provenientes de la tabla de referencia
    
    Parámetros
    ----------
    df_enriched : pd.DataFrame
        DataFrame resultante del enriquecimiento dinámico + estático.
    
    Retorna
    -------
    pd.DataFrame
        DataFrame final listo para análisis, visualización o exportación.
    """

    df = df_enriched.copy()

    # Columnas operativas principales
    cols_operativas = [
        "icao24", "callsign", "origin_country",
        "snapshot_time", "snapshot_hour",
        "latitude", "longitude",
        "baro_altitude", "geo_altitude",
        "velocity", "true_track", "vertical_rate",
        "on_ground", "squawk"
    ]

    # Columnas de metadatos estáticos
    cols_metadata = [
        "manufacturername", "model", "typecode",
        "built", "owner", "engines"
    ]

    # Se filtran solo columnas existentes para evitar errores
    columnas_finales = [col for col in cols_operativas + cols_metadata if col in df.columns]

    df_final = df[columnas_finales].copy()

    return df_final