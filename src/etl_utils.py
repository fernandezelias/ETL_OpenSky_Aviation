# ==========================================================
# etl_utils.py — ETL_OpenSky_Aviation
# ==========================================================

# --- Librerías estándar ---
from datetime import datetime
import os

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
    Retorna un dict con claves ["time", "states"].
    """
    try:
        response = requests.get(base_url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al obtener datos de OpenSky: {e}")
        return None


def get_aircraft_metadata_csv(path: str) -> pd.DataFrame:
    """
    Carga metadatos estáticos de aeronaves desde un CSV local.
    """
    return pd.read_csv(path)



# ==========================================================
# 2. TRANSFORMACIÓN DE DATOS — SNAPSHOT DINÁMICO
# ==========================================================

def normalize_opensky(json_data):
    """
    Convierte la lista de estados en un DataFrame tabular.
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
    df = df.drop(columns=["sensors"], errors="ignore")

    return df



# ==========================================================
# 3. TRANSFORMACIÓN SILVER — METADATOS ESTÁTICOS  (NUEVO)
# ==========================================================

def clean_static_aircraft_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y tipifica la tabla de metadatos estáticos antes de guardarla en Silver.

    Realiza:
    - estandarización de columnas
    - tipificación de ICAO24 como string
    - coerción de fechas y columnas numéricas
    """
    df = df.copy()

    # Nombres estandarizados
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # Asegurar tipo string en clave principal
    if "icao24" in df.columns:
        df["icao24"] = df["icao24"].astype("string")

    # Columnas numéricas típicas
    numeric_cols = ["built", "engines"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Fechas
    if "built" in df.columns:
        try:
            df["built"] = pd.to_datetime(df["built"], errors="coerce")
        except:
            pass

    return df



# ==========================================================
# 4. GUARDADO Y LECTURA EN DELTA LAKE
# ==========================================================

def save_data_as_delta(df, path, mode="overwrite", partition_cols=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    write_deltalake(
        path,
        df,
        mode=mode,
        partition_by=partition_cols
    )

    print(f"💾 Datos guardados en Delta Lake: {path}")


def save_new_data_as_delta(new_data, data_path, predicate, partition_cols=None):
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


def read_all_from_delta(path):
    dt = DeltaTable(path)
    return dt.to_pandas()


# ==========================================================
# 5. TRANSFORMACIONES GOLD
# ==========================================================

def prepare_states_for_gold(df_states_silver):
    df = df_states_silver.copy()

    cols_string = ["icao24", "callsign", "origin_country", "squawk"]
    for col in cols_string:
        if col in df.columns:
            df[col] = df[col].astype("string")

    if "snapshot_time" in df.columns:
        df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], errors="coerce")

    return df


def enrich_states_with_metadata(df_states, df_aircraft):
    df_states = df_states.copy()
    df_aircraft = df_aircraft.copy()

    df_states["icao24"] = df_states["icao24"].astype("string")
    df_aircraft["icao24"] = df_aircraft["icao24"].astype("string")

    return df_states.merge(
        df_aircraft,
        on="icao24",
        how="left",
        suffixes=("", "_meta")
    )


def build_gold_final_dataset(df_enriched):
    df = df_enriched.copy()

    cols_operativas = [
        "icao24", "callsign", "origin_country",
        "snapshot_time", "snapshot_hour",
        "latitude", "longitude",
        "baro_altitude", "geo_altitude",
        "velocity", "true_track", "vertical_rate",
        "on_ground", "squawk"
    ]

    cols_metadata = [
        "manufacturername", "model", "typecode",
        "built", "owner", "engines"
    ]

    columnas_finales = [c for c in cols_operativas + cols_metadata if c in df.columns]

    return df[columnas_finales].copy()