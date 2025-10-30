# simem_extract_clean.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode, quote_plus

import pandas as pd
import requests


# ==================================================
# PARAMS
# ==================================================

PARAMS = {
    # --- Qué dataset descargar
    
    "dataset_id": "a704ee",

    # --- Rango de fechas que entiende la API (usualmente YYYY-MM-DD):
    "start_date": "2025-07-01",
    "end_date":   "2025-10-30",

    # --- Filtros opcionales (server-side). Déjalos como None si no aplican:
    "column_destiny_name": None,      # ej: "Recurso"
    "values": None,                   # ej: "Gas,Carbón" o ["Gas","Carbón"]

    # --- Limpieza
    "to_snake_case": True,            # renombrar columnas a snake_case
    "strip_text": True,               # .str.strip() en texto
    "drop_duplicates": True,
    "drop_all_null_columns": True,    # elimina columnas completamente nulas
    "subset_columns": None,           # ej: ["Id","Fecha","Valor"] o ["id","fecha","valor"] si snake_case

    # --- Filtros client-side (por si no aplica el server o quieres más control)
    "client_filters": {
        # igualdad / pertenencia
        # "columna": ["v1","v2"]
    },
    # Filtrado por fecha client-side (si conoces la columna de fecha dentro del dataset)
    "client_date_filter": {
        "date_column": None,          # ej: "Fecha" o "fecha" si snake_case
        "start": None,                # "YYYY-MM-DD" o None
        "end": None,                  # "YYYY-MM-DD" o None (inclusive)
    },

    # --- Salidas
    "output_csv": None,               # ej: "out/datos.csv"
    "output_parquet": None,           # ej: "out/datos.parquet"

    # --- Red/robustez
    "timeout_secs": 60,
    "max_retries": 3,
    "retry_backoff_secs": 2,
}

BASE_URL = "https://www.simem.co/backend-files/api/PublicData"


# ==================================================
# Utilidades
# ==================================================

def _ensure_values_list(values: Optional[Any]) -> Optional[List[str]]:
    if values is None:
        return None
    if isinstance(values, str):
        items = [s.strip() for s in values.split(",") if s.strip()]
        return items or None
    if isinstance(values, (list, tuple, set)):
        items = [str(x).strip() for x in values if str(x).strip()]
        return items or None
    return None


def _snake_case(name: str) -> str:
    import re
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    s = re.sub(r"[\s\-\/]+", "_", s)
    s = s.replace("__", "_")
    return s.lower()


def _build_query(dataset_id: str, start_date: str, end_date: str,
                 column_destiny_name: Optional[str],
                 values_list: Optional[List[str]]) -> str:
    q = {
        "startDate": start_date,
        "endDate": end_date,
        "datasetId": dataset_id,
        "columnDestinyName": "null" if not column_destiny_name else column_destiny_name,
        "values": "null" if not values_list else ",".join(values_list),
    }
    return f"{BASE_URL}?{urlencode(q, quote_via=quote_plus)}"


def _http_get_with_retries(url: str, timeout: int, max_retries: int, backoff: int) -> requests.Response:
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, timeout=timeout)
            if 200 <= r.status_code < 300:
                return r
            last_exc = RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
        except Exception as e:
            last_exc = e
        if attempt < max_retries:
            time.sleep(backoff * attempt)
    raise last_exc


def _map_types_by_schema(df: pd.DataFrame, schema_columns: List[Dict[str, Any]],
                         to_snake: bool, strip_text: bool) -> pd.DataFrame:
    """Tipifica columnas usando el bloque result.columns (dataType: 'texto' | 'fecha' | 'fecha hora')."""
    def _norm(s: str) -> str:
        return s.strip().lower()

    idx = { _norm(c.get("nameColumn", "")): (c.get("nameColumn"), (c.get("dataType") or "").lower())
            for c in schema_columns if c.get("nameColumn") }

    for col in list(df.columns):
        key = _norm(col)
        _, dtype = idx.get(key, (None, None))
        if dtype == "fecha":
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        elif dtype == "fecha hora":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        else:
            df[col] = df[col].astype("string")
            if strip_text:
                df[col] = df[col].str.strip()

    if to_snake:
        df = df.rename(columns={c: _snake_case(c) for c in df.columns})
    return df


def _fallback_infer_types(df: pd.DataFrame, to_snake: bool, strip_text: bool) -> pd.DataFrame:
    """Si el schema no viene o está incompleto, intenta inferencia segura."""
    for col in list(df.columns):
        # intenta fecha-hora
        parsed = pd.to_datetime(df[col], errors="coerce", utc=False)
        if parsed.notna().mean() > 0.9:
            df[col] = parsed
            continue
        # texto seguro
        if df[col].dtype == object:
            df[col] = df[col].astype("string")
            if strip_text:
                df[col] = df[col].str.strip()
    if to_snake:
        df = df.rename(columns={c: _snake_case(c) for c in df.columns})
    return df


def _apply_client_filters(df: pd.DataFrame,
                          eq_filters: Dict[str, List[str]],
                          to_snake: bool) -> pd.DataFrame:
    if not eq_filters:
        return df
    res = df
    for col, vals in eq_filters.items():
        vals = [str(v).strip() for v in vals]
        col_eff = _snake_case(col) if to_snake else col
        if col_eff not in res.columns:
            # intenta case-insensitive
            candidates = [c for c in res.columns if c.lower() == col_eff.lower()]
            if candidates:
                col_eff = candidates[0]
            else:
                print(f"[WARN] Filtro ignorado: columna '{col}' no encontrada.")
                continue
        series = res[col_eff]
        mask = series.astype(str).str.strip().isin(vals)
        res = res.loc[mask].copy()
    return res


def _apply_client_date_filter(df: pd.DataFrame,
                              conf: Dict[str, Optional[str]],
                              to_snake: bool) -> pd.DataFrame:
    if not conf or not conf.get("date_column"):
        return df
    col = conf["date_column"]
    col_eff = _snake_case(col) if to_snake else col
    if col_eff not in df.columns:
        candidates = [c for c in df.columns if c.lower() == col_eff.lower()]
        if candidates:
            col_eff = candidates[0]
        else:
            print(f"[WARN] Filtro por fecha ignorado: columna '{col}' no encontrada.")
            return df

    s = df[col_eff]
    # conviértelo si aún no lo es
    if not pd.api.types.is_datetime64_any_dtype(s) and not pd.api.types.is_datetime64tz_dtype(s):
        s = pd.to_datetime(s, errors="coerce")
    start = conf.get("start")
    end = conf.get("end")
    mask = pd.Series(True, index=df.index)
    if start:
        mask &= s >= pd.to_datetime(start)
    if end:
        # inclusivo
        mask &= s <= pd.to_datetime(end) + pd.Timedelta(days=0, hours=23, minutes=59, seconds=59, microseconds=999999)
    return df.loc[mask].copy()


# ==================================================
# Núcleo
# ==================================================

@dataclass
class SimemResult:
    raw_json: Dict[str, Any]
    df: pd.DataFrame
    metadata: Dict[str, Any]
    schema_columns: List[Dict[str, Any]]


def fetch_dataset_and_clean(params: Dict[str, Any]) -> SimemResult:
    dataset_id = params["dataset_id"]
    start_date = params["start_date"]
    end_date = params["end_date"]
    col_dest = params.get("column_destiny_name")
    values_list = _ensure_values_list(params.get("values"))
    to_snake = bool(params.get("to_snake_case", True))
    strip_text = bool(params.get("strip_text", True))
    drop_dups = bool(params.get("drop_duplicates", True))
    drop_all_null_cols = bool(params.get("drop_all_null_columns", True))
    subset_cols = params.get("subset_columns")
    client_filters = params.get("client_filters") or {}
    client_date_filter = params.get("client_date_filter") or {}
    timeout = int(params.get("timeout_secs", 60))
    max_retries = int(params.get("max_retries", 3))
    backoff = int(params.get("retry_backoff_secs", 2))

    url = _build_query(dataset_id, start_date, end_date, col_dest, values_list)
    print(f"[INFO] GET {url}")

    resp = _http_get_with_retries(url, timeout=timeout, max_retries=max_retries, backoff=backoff)
    try:
        payload = resp.json()
    except Exception:
        raise RuntimeError(f"Respuesta no JSON: {resp.text[:300]}")

    if not payload.get("success", False):
        raise RuntimeError(f"API respondió success=false: {json.dumps(payload)[:300]}")

    result = payload.get("result") or {}
    records = result.get("records") or []
    schema_columns = result.get("columns") or []
    metadata = {k: v for k, v in result.items() if k not in ("records", "columns")}

    # A veces el dataset devuelve listas anidadas o dicts por fila; normaliza siempre
    df = pd.json_normalize(records, sep="_")

    # Tipificación
    if schema_columns:
        df = _map_types_by_schema(df, schema_columns, to_snake=to_snake, strip_text=strip_text)
    else:
        df = _fallback_infer_types(df, to_snake=to_snake, strip_text=strip_text)

    # Limpiezas
    if drop_all_null_cols and not df.empty:
        df = df.dropna(axis=1, how="all")
    if drop_dups and not df.empty:
        df = df.drop_duplicates().reset_index(drop=True)

    # Filtros client-side
    if client_filters:
        df = _apply_client_filters(df, client_filters, to_snake=to_snake)
    if client_date_filter and (client_date_filter.get("start") or client_date_filter.get("end")):
        df = _apply_client_date_filter(df, client_date_filter, to_snake=to_snake)

    # Subset columnas (después de renombrar)
    if subset_cols:
        # respeta snake_case si está activado
        final_cols = [(_snake_case(c) if to_snake else c) for c in subset_cols]
        missing = [c for c in final_cols if c not in df.columns]
        if missing:
            print(f"[WARN] Columnas faltantes en subset: {missing}. Se ignorarán.")
        keep = [c for c in final_cols if c in df.columns]
        if keep:
            df = df[keep].copy()

    return SimemResult(raw_json=payload, df=df, metadata=metadata, schema_columns=schema_columns)


def save_outputs(df: pd.DataFrame, csv_path: Optional[str], parquet_path: Optional[str]) -> None:
    if csv_path:
        df.to_csv(csv_path, index=False)
        print(f"[INFO] CSV guardado: {csv_path}")
    if parquet_path:
        df.to_parquet(parquet_path, index=False)
        print(f"[INFO] Parquet guardado: {parquet_path}")


# ==================================================
# API pública del módulo
# ==================================================

def get_df(params: Dict[str, Any] = PARAMS) -> pd.DataFrame:
    """Devuelve el DataFrame limpio (para notebooks/consola)."""
    return fetch_dataset_and_clean(params).df


def main() -> pd.DataFrame:
    res = fetch_dataset_and_clean(PARAMS)

    dfinMemory = res.df.copy()

    print("[INFO] Metadatos:")
    print(json.dumps({
        "dataset_id": PARAMS["dataset_id"],
        "name": res.metadata.get("name"),
        "filterDate": res.metadata.get("filterDate"),
        "n_records": len(res.df)
    }, ensure_ascii=False, indent=2))

    print("[INFO] Esquema (nameColumn -> dataType):")
    for c in res.schema_columns:
        print(f"  - {c.get('nameColumn')}: {c.get('dataType')}")

    # Guardar si se configuró
    save_outputs(
        res.df,
        csv_path=PARAMS.get("output_csv"),
        parquet_path=PARAMS.get("output_parquet"),
    )

    print("[INFO] Preview:")
    with pd.option_context("display.max_columns", None, "display.width", 160):
        print(res.df.head(10))

    return dfinMemory


if __name__ == "__main__":
    try:
       dfinMemory = main()
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        sys.exit(1)
