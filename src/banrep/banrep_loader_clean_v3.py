# banrep_consolidate_v3.py
# -*- coding: utf-8 -*-
"""
Consolida CSVs (uno por flow) en:
- df_long (largo/tidy)
- df_wide (ancho: columnas = "FLOW_ID :: series_name")
Estandariza dtypes para evitar errores de PyArrow con la columna 'time'.
"""

from __future__ import annotations
import argparse, logging, os, re, sys, time, glob
from typing import List, Optional, Tuple
import pandas as pd

# ========= PARAMS (ajusta aquí) =========
OUT_DIR_DEFAULT     = "banrep_output"                  # raíz con /data y /catalog
FLOWS_DEFAULT       = "ALL"                            # "ALL" o lista: ["DF_TRM_DAILY_HIST", ...]
SAVE_PARQUET_LONG   = "banrep_output/catalog/all_long.parquet"  # None para no guardar
SAVE_PARQUET_WIDE   = "banrep_output/catalog/all_wide.parquet"  # None para no guardar
SAVE_CSV_LONG       = None                             # Ej: "banrep_output/catalog/all_long.csv"
SAVE_CSV_WIDE       = None                             # Ej: "banrep_output/catalog/all_wide.csv"
LOG_LEVEL_DEFAULT   = "INFO"
# ========================================

# ----- Logger sin duplicados -----
logger = logging.getLogger("banrep_cons")
if not logger.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s - %(message)s", "%H:%M:%S"))
    logger.addHandler(h)
logger.setLevel(getattr(logging, LOG_LEVEL_DEFAULT, logging.INFO))
logger.propagate = False

def set_log_level(level: str = "INFO") -> None:
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

def log_call(fn):
    def wrapper(*args, **kwargs):
        t0 = time.perf_counter()
        try:
            r = fn(*args, **kwargs)
            dt = (time.perf_counter() - t0) * 1000
            if isinstance(r, pd.DataFrame):
                logger.debug("← %s() OK (%.1f ms, df=%s×%s)", fn.__name__, dt, *r.shape)
            else:
                logger.debug("← %s() OK (%.1f ms)", fn.__name__, dt)
            return r
        except Exception as e:
            dt = (time.perf_counter() - t0) * 1000
            logger.error("× %s() FAIL (%.1f ms): %s", fn.__name__, dt, e)
            raise
    return wrapper

# ----- Utils -----
def _snake(s: str) -> str:
    return re.sub(r"[^0-9a-zA-Z]+", "_", str(s)).strip("_").lower()

@log_call
def list_available_flows(out_dir: str) -> List[str]:
    data_dir = os.path.join(out_dir, "data")
    files = glob.glob(os.path.join(data_dir, "*.csv"))
    flows = sorted([os.path.splitext(os.path.basename(p))[0] for p in files])
    logger.info("Encontré %s CSV en %s", len(flows), data_dir)
    return flows

@log_call
def load_one_flow_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Asegura 'flow_id'
    if "flow_id" not in df.columns:
        df["flow_id"] = os.path.splitext(os.path.basename(path))[0]

    # Construye 'date' si falta
    if "date" not in df.columns and "time" in df.columns:
        s = df["time"].astype(str)
        cleaned = s.str.replace(r"[^0-9]", "", regex=True)
        dt = pd.to_datetime(cleaned, format="%Y%m%d", errors="coerce")
        if dt.isna().all():
            dt = pd.to_datetime(cleaned, format="%Y%m", errors="coerce")
        if dt.isna().all():
            dt = pd.to_datetime(cleaned, format="%Y", errors="coerce")
        df["date"] = dt

    # Asegura columnas clave
    for must in ("time","date","value","series_name","flow_id"):
        if must not in df.columns:
            if must == "value" and "OBS_VALUE" in df.columns:
                df["value"] = df["OBS_VALUE"]
            else:
                df[must] = pd.NA
    return df

def _enforce_schema_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza tipos para parquet:
    - date -> datetime64[ns]
    - value -> float64
    - time, flow_id, series_name, *_code, *_name y demás -> string (no object)
    """
    out = df.copy()

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")

    if "value" in out.columns:
        out["value"] = pd.to_numeric(out["value"], errors="coerce")

    if "time" in out.columns:
        # MUY IMPORTANTE: mantener como string, evita que PyArrow intente int64
        out["time"] = out["time"].astype("string")

    # Todo lo que no sea date/value -> string
    for c in out.columns:
        if c in ("date", "value"):
            continue
        if pd.api.types.is_categorical_dtype(out[c]):
            out[c] = out[c].astype("string")
        elif not pd.api.types.is_string_dtype(out[c]):
            # evita convertir NaN a 'nan'
            out[c] = out[c].astype("string")

    return out

def _safe_to_parquet(df: pd.DataFrame, path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        df.to_parquet(path, index=False)  # engine=pyarrow por defecto
        logger.info("Guardado Parquet → %s", path)
        return path
    except Exception as e:
        logger.warning("Parquet falló (%s). Hago fallback a CSV…", e)
        alt = re.sub(r"\.parquet$", ".csv", path)
        df.to_csv(alt, index=False)
        logger.info("Guardado CSV (fallback) → %s", alt)
        return alt

# ----- Consolidación -----
@log_call
def consolidate(
    out_dir: str,
    flows: Optional[List[str]] = None,
    save_parquet_long: Optional[str] = SAVE_PARQUET_LONG,
    save_parquet_wide: Optional[str] = SAVE_PARQUET_WIDE,
    save_csv_long: Optional[str] = SAVE_CSV_LONG,
    save_csv_wide: Optional[str] = SAVE_CSV_WIDE,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    data_dir = os.path.join(out_dir, "data")
    if flows is None or flows == [] or (isinstance(flows, str) and flows.upper() == "ALL"):
        flows = list_available_flows(out_dir)
    else:
        flows = [str(f).strip() for f in flows if str(f).strip()]
    if not flows:
        raise RuntimeError("No hay variables para consolidar (revisa out_dir/data o la lista de flows).")

    # Carga y concatena (largo)
    dfs = []
    for fid in flows:
        path = os.path.join(data_dir, f"{fid}.csv")
        if not os.path.exists(path):
            logger.warning("No existe CSV para %s (omito). Esperado: %s", fid, path)
            continue
        df = load_one_flow_csv(path)
        df["flow_id"] = fid
        # Orden de columnas
        first = [c for c in ("date","time","value","series_name","flow_id") if c in df.columns]
        rest  = [c for c in df.columns if c not in first]
        df = df[first + rest]
        dfs.append(df)

    if not dfs:
        raise RuntimeError("No se pudo cargar ningún CSV. ¿Ruta correcta y permisos?")

    df_long = pd.concat(dfs, ignore_index=True).sort_values(["flow_id","date","time"]).reset_index(drop=True)

    # Tipado seguro antes de guardar
    df_long = _enforce_schema_strings(df_long)

    # Ancho
    tmp = df_long.copy()
    tmp["_col_"] = tmp.apply(
        lambda r: f"{r['flow_id']} :: {r['series_name']}" if pd.notna(r.get("series_name")) else f"{r['flow_id']} :: series",
        axis=1
    )
    idx = "date" if "date" in tmp.columns else "time"
    df_wide = tmp.pivot_table(index=idx, columns="_col_", values="value", aggfunc="first").sort_index().reset_index()

    # También tipa seguro el ancho (date dt64, resto float/string)
    if "date" in df_wide.columns:
        df_wide["date"] = pd.to_datetime(df_wide["date"], errors="coerce")
    for c in df_wide.columns:
        if c == "date":
            continue
        # columnas de series deben ser numéricas (o NaN)
        if c != "time":
            df_wide[c] = pd.to_numeric(df_wide[c], errors="coerce")

    # Guardar
    os.makedirs(os.path.join(out_dir, "catalog"), exist_ok=True)
    _safe_to_parquet(df_long, save_parquet_long)
    _safe_to_parquet(df_wide, save_parquet_wide)
    if save_csv_long:
        df_long.to_csv(save_csv_long, index=False); logger.info("Guardado CSV → %s", save_csv_long)
    if save_csv_wide:
        df_wide.to_csv(save_csv_wide, index=False); logger.info("Guardado CSV → %s", save_csv_wide)

    return df_long, df_wide

# ----- CLI -----
def build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Consolidar CSVs de BanRep en DataFrame largo/ancho, con dtypes seguros.")
    p.add_argument("--out-dir", default=OUT_DIR_DEFAULT, help="Directorio raíz con /data y /catalog")
    p.add_argument("--flows", default=FLOWS_DEFAULT,
                   help='Lista separada por comas de FLOW_ID, o "ALL" (default) para todas.')
    p.add_argument("--flows-from-file", help="Archivo TXT/CSV/JSON con columna/array flow_id")
    p.add_argument("--save-parquet-long", default=SAVE_PARQUET_LONG)
    p.add_argument("--save-parquet-wide", default=SAVE_PARQUET_WIDE)
    p.add_argument("--save-csv-long", default=SAVE_CSV_LONG)
    p.add_argument("--save-csv-wide", default=SAVE_CSV_WIDE)
    p.add_argument("--log", default=LOG_LEVEL_DEFAULT, help="DEBUG|INFO|WARNING|ERROR")
    return p

def read_flows_from_file(path: str) -> List[str]:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".txt":
        return [ln.strip() for ln in open(path, "r", encoding="utf-8") if ln.strip() and not ln.strip().startswith("#")]
    if ext == ".json":
        import json
        data = json.load(open(path, "r", encoding="utf-8"))
        if isinstance(data, list) and all(isinstance(x, str) for x in data): return data
        if isinstance(data, list) and all(isinstance(x, dict) and "flow_id" in x for x in data): return [x["flow_id"] for x in data]
        raise ValueError("JSON no reconocido: usa array de strings o de objetos con 'flow_id'.")
    df = pd.read_csv(path)
    for cand in ["flow_id","Flow","FLOW_ID","id","Id"]:
        if cand in df.columns:
            return [str(x) for x in df[cand].dropna().astype(str).tolist()]
    return [str(x) for x in df.iloc[:,0].dropna().astype(str).tolist()]

def main(argv=None):
    args = build_cli().parse_args(argv)
    set_log_level(args.log)

    # Resolver lista de flows
    if args.flows_from_file:
        flows = read_flows_from_file(args.flows_from_file)
    else:
        flows = None if args.flows.strip().upper() == "ALL" else [s.strip() for s in args.flows.split(",") if s.strip()]

    df_long, df_wide = consolidate(
        out_dir=args.out_dir,
        flows=flows,
        save_parquet_long=args.save_parquet_long or None,
        save_parquet_wide=args.save_parquet_wide or None,
        save_csv_long=args.save_csv_long or None,
        save_csv_wide=args.save_csv_wide or None,
    )

    # Variables en el entorno interactivo (si usas python -i)
    import __main__
    __main__.df_long = df_long
    __main__.df_wide = df_wide
    logger.info("OK ✓ df_long y df_wide disponibles en el entorno.")

if __name__ == "__main__":
    if len(sys.argv) == 1:
        demo = [
            "--out-dir", OUT_DIR_DEFAULT,
            "--flows", FLOWS_DEFAULT,
            "--save-parquet-long", SAVE_PARQUET_LONG or "",
            "--save-parquet-wide", SAVE_PARQUET_WIDE or "",
            "--log", LOG_LEVEL_DEFAULT,
        ]
        main(demo)
    else:
        main()



