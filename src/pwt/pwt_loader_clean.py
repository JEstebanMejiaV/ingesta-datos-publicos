# pwt_loader_clean.py
# -*- coding: utf-8 -*-
"""
Carga Penn World Table (PWT 11.x) desde Dataverse (por DOI), normaliza y genera:
- pwt_main     : panel tidy (iso, country, year, variables)
- vista_figura : filas (iso,country,variable) x columnas=años
- wide_panel   : igual a pwt_main (tras filtros, si aplica)

Uso como librería:
    from pwt_loader_clean import load_pwt, export_csv
    pwt_main, vista_figura, wide_panel = load_pwt(out_dir="pwt_out")
    export_csv(pwt_main, vista_figura, wide_panel, "pwt_out")

Uso como script (y quedarte con variables):
    python -i pwt_loader_clean.py  # entra a modo interactivo con los df en memoria

Requiere: pandas, requests, openpyxl (xlsx), pyreadstat (opcional para .dta)
"""
from __future__ import annotations

import hashlib
import io
import logging
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -------------------- Config por defecto --------------------
BASE = "https://dataverse.nl"
DOI  = "doi:10.34894/FABVLR"  # PWT 11.x
TIMEOUT = (10, 180)           # (connect, read) seconds
USER_AGENT = "pwt-loader/clean-1.0"
API_TOKEN = os.getenv("DATAVERSE_API_TOKEN", "").strip()

# -------------------- Logger (sin duplicados) --------------------
logger = logging.getLogger("pwt_loader")
if not logger.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s - %(message)s", "%H:%M:%S"))
    logger.addHandler(_h)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))
logger.propagate = False  # evita duplicación si el root también tiene handler

# --------------------- Etiquetas de variables ) ----------------------

VAR_LABELS: Dict[str, str] = {
    # Tus entradas
    "cgdpo": "Output-side real GDP at current PPPs (in mil. 2021US$)",
    "rgdpe": "Expenditure-side real GDP at chained PPPs (in mil. 2017US$)",
    "rgdpo": "Output-side real GDP at chained PPPs (in mil. 2017US$)",
    "pop"  : "Population (in millions)",
    "emp"  : "Number of persons engaged (in millions)",
    "avh"  : "Average annual hours worked by persons engaged",
    "hc"   : "Human capital index (years of schooling & returns)",
    "labsh": "Share of labour compensation in GDP (national prices)",
    "ctfp" : "TFP level at current PPPs (USA=1)",
    "ck"   : "Capital services levels at current PPPs (USA=1)",

    # PIB y componentes a PPP corrientes (niveles) – base 2021 en PWT 11
    "cgdpe": "Expenditure-side real GDP at current PPPs (in mil. 2021US$)",
    "ccon" : "Real consumption of households and government at current PPPs (in mil. 2021US$)",
    "cda"  : "Real domestic absorption at current PPPs (in mil. 2021US$)",
    "cn"   : "Capital stock at current PPPs (in mil. 2021US$)",
    "cwtfp": "Welfare-relevant TFP level at current PPPs (USA=1)",

    # Series a precios nacionales constantes (crecimiento) – base 2017
    "rgdpna": "Real GDP at constant national prices (in mil. 2017US$)",
    "rconna": "Real consumption at constant national prices (in mil. 2017US$)",
    "rdana" : "Real domestic absorption at constant national prices (in mil. 2017US$)",
    "rnna"  : "Capital stock at constant national prices (in mil. 2017US$)",
    "rkna"  : "Capital services at constant national prices (2017=1)",
    "rtfpna": "TFP at constant national prices (2017=1)",
    "rwtfpna": "Welfare-relevant TFP at constant national prices (2017=1)",

    # Shares de gasto (fracciones de CGDPo, PPP corrientes)
    "csh_c": "Share of household consumption at current PPPs",
    "csh_i": "Share of gross capital formation at current PPPs",
    "csh_g": "Share of government consumption at current PPPs",
    "csh_x": "Share of merchandise exports at current PPPs",
    "csh_m": "Share of merchandise imports at current PPPs",
    "csh_r": "Share of residual trade and statistical discrepancy at current PPPs",

    # Niveles de precios (PPP/XR), referidos a USA GDPo = 1 en año base
    "pl_con": "Price level of CCON (PPP/XR), USA GDPo in base year = 1",
    "pl_da" : "Price level of CDA (PPP/XR), USA GDPo in base year = 1",
    "pl_gdpo": "Price level of CGDPo (PPP/XR), USA GDPo in base year = 1",
    "pl_gdpe": "Price level of CGDPe (PPP/XR), USA GDPo in base year = 1",
    "pl_c"  : "Price level of household consumption (USA GDPo base = 1)",
    "pl_i"  : "Price level of capital formation (USA GDPo base = 1)",
    "pl_g"  : "Price level of government consumption (USA GDPo base = 1)",
    "pl_x"  : "Price level of exports (USA GDPo base = 1)",
    "pl_m"  : "Price level of imports (USA GDPo base = 1)",
    "pl_n"  : "Price level of the capital stock (USA base = 1)",
    "pl_k"  : "Price level of capital services (USA = 1)",

    # Información adicional / calidad de datos
    "xr"      : "Exchange rate, national currency per USD (market+estimated)",
    "irr"     : "Real internal rate of return",
    "delta"   : "Average depreciation rate of the capital stock",
    "i_cig"   : "Flag: relative price data for C, I, G (0=extrap., 1=benchmark, 2=interp., 3=ICP timeseries bench/interp., 4=ICP timeseries extrap.)",
    "i_xm"    : "Flag: relative price data for exports/imports (0=extrap., 1=benchmark, 2=interp.)",
    "i_xr"    : "Flag: exchange rate is market-based (0) or estimated (1)",
    "i_outlier": "Flag: observation on pl_gdpe/pl_gdpo is not an outlier (0) or an outlier (1)",
    "i_irr"   : "Flag: irr regular (0), biased (1), at 1% lower bound (2), or outlier (3)",
    "cor_exp" : "Correlation of expenditure shares with the US (benchmark years only)",
    "statcap" : "Statistical capacity indicator (World Bank; developing countries only)",

    # Identificadores / metadatos comunes en PWT
    "country"   : "Country name",
    "isocode"   : "ISO 3166-1 alpha-3 country code",
    "year"      : "Year",
    "currency"  : "National currency unit"
}

# -------------------- Excepciones --------------------
class DataverseError(RuntimeError): ...
class ValidationError(RuntimeError): ...

# -------------------- Dataclasses & sesión --------------------
@dataclass(frozen=True)
class FileMeta:
    id: int
    label: str
    content_type: str = ""
    size: Optional[int] = None
    checksum: Optional[str] = None  # md5 si lo provee el server

def _mk_session() -> requests.Session:
    s = requests.Session()
    headers = {"User-Agent": USER_AGENT}
    if API_TOKEN:
        headers["X-Dataverse-key"] = API_TOKEN
    s.headers.update(headers)
    retry = Retry(
        total=4, connect=4, read=4, status=4,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    return s

SESSION = _mk_session()

# -------------------- Utilidades Dataverse --------------------
def _safe_name(name: str) -> str:
    return re.sub(r"[^\w\-.]+", "_", name.strip())

def list_files_latest_published(base: str, persistent_id: str) -> List[FileMeta]:
    url = f"{base}/api/datasets/:persistentId/versions/:latest-published/files"
    r = SESSION.get(url, params={"persistentId": persistent_id}, timeout=TIMEOUT)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        raise DataverseError(f"HTTP {r.status_code} al listar archivos: {e}") from e
    try:
        js = r.json()
    except Exception as e:
        raise DataverseError("Respuesta no-JSON al listar archivos.") from e
    data = js.get("data")
    if not isinstance(data, list):
        raise DataverseError("Estructura inesperada en la respuesta de Dataverse.")
    metas: List[FileMeta] = []
    for item in data:
        df = item.get("dataFile") or {}
        if "id" not in df:
            continue
        checksum = None
        if isinstance(df.get("checksum"), dict):
            checksum = df["checksum"].get("value")
        metas.append(FileMeta(
            id=int(df["id"]),
            label=item.get("label", "unknown"),
            content_type=df.get("contentType", "") or "",
            size=df.get("filesize"),
            checksum=checksum
        ))
    logger.info("Archivos en la versión publicada: %d", len(metas))
    return metas

def download_file_by_id(base: str, file_id: int, original: bool = True) -> bytes:
    url = f"{base}/api/access/datafile/{file_id}"
    params = {"format": "original"} if original else {}
    r = SESSION.get(url, params=params, timeout=TIMEOUT, stream=True)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        if original:
            logger.warning("Fallo con format=original; reintentando sin formato…")
            return download_file_by_id(base, file_id, original=False)
        raise
    return r.content

# -------------------- Lectores --------------------
def _read_excel_main_sheet(raw: bytes) -> pd.DataFrame:
    xls = pd.ExcelFile(io.BytesIO(raw))
    sheet = "Data" if "Data" in xls.sheet_names else xls.sheet_names[0]
    logger.info("Excel → usando hoja: %s", sheet)
    return pd.read_excel(xls, sheet_name=sheet)

def _read_stata(raw: bytes) -> pd.DataFrame:
    return pd.read_stata(io.BytesIO(raw))

# -------------------- Core --------------------
def _prefer_main_file(metas: List[FileMeta]) -> Tuple[Optional[FileMeta], Optional[FileMeta]]:
    dta = next((m for m in metas if re.match(r"^pwt\d+\.dta$", m.label, re.I)), None)
    xls = next((m for m in metas if re.match(r"^pwt\d+\.xlsx$", m.label, re.I)), None)
    return dta, xls

def _maybe_use_cache(out_dir: Path, meta: FileMeta) -> Optional[bytes]:
    p = out_dir / _safe_name(meta.label)
    if not p.exists():
        return None
    if meta.checksum:
        md5 = hashlib.md5(p.read_bytes()).hexdigest()
        if md5 == meta.checksum:
            logger.info("Cache OK para %s (md5 coincide).", meta.label)
            return p.read_bytes()
        logger.info("Cache desactualizada para %s (md5 no coincide).", meta.label)
        return None
    logger.info("Cache encontrada para %s (sin checksum remoto).", meta.label)
    return p.read_bytes()

def _normalize_panel(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    iso  = next((cols[c] for c in ("isocode","countrycode","iso","code") if c in cols), None)
    cty  = next((cols[c] for c in ("country","cty") if c in cols), None)
    year = next((cols[c] for c in ("year","yr") if c in cols), None)
    if not all([iso, cty, year]):
        raise ValidationError(f"No detecté columnas clave (iso/country/year).")
    df = df.rename(columns={iso:"iso", cty:"country", year:"year"})
    df["iso"]     = df["iso"].astype("string")
    df["country"] = df["country"].astype("string")
    df["year"]    = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    order = ["iso","country","year"] + [c for c in df.columns if c not in ("iso","country","year")]
    return df[order].sort_values(["iso","year"]).reset_index(drop=True)


def _build_views(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    keys = {"iso","country","year"}
    var_cols = [c for c in df.columns if c not in keys]
    # wide_panel
    wide_panel = df.copy()
    # vista_figura (años como columnas)
    long = df.melt(id_vars=["iso","country","year"],
                   value_vars=var_cols,
                   var_name="variable_code", value_name="value")
    long["variable_name"] = long["variable_code"].map(VAR_LABELS).fillna(long["variable_code"])
    vista_figura = (
        long.pivot_table(index=["iso","country","variable_code","variable_name"],
                         columns="year", values="value", aggfunc="first")
        .reset_index()
        .sort_values(["iso","variable_code"])
    )
    meta = ["iso","country","variable_code","variable_name"]
    years = sorted([c for c in vista_figura.columns if isinstance(c,(int,float)) or str(c).isdigit()],
                   key=lambda x: int(x))
    vista_figura = vista_figura[meta + years]
    return vista_figura, wide_panel

# -------------------- API pública --------------------
def load_pwt(
    base: str = BASE,
    doi: str = DOI,
    out_dir: str | Path = "pwt_out",
    use_cache: bool = True,
    countries: Optional[List[str]] = None,
    keep_vars: Optional[List[str]] = None,
    downcast: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Devuelve (pwt_main, vista_figura, wide_panel). También guarda el archivo crudo en out_dir.
    """
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)

    metas = list_files_latest_published(base, doi)
    dta, xls = _prefer_main_file(metas)
    if not (dta or xls):
        raise DataverseError("No encontré pwt*.dta ni pwt*.xlsx en el dataset.")
    meta = dta or xls

    raw = _maybe_use_cache(out, meta) if use_cache else None
    if raw is None:
        logger.info("Descargando %s ...", meta.label)
        raw = download_file_by_id(base, meta.id, original=True)
        (out / _safe_name(meta.label)).write_bytes(raw)
        logger.info("Guardado archivo crudo en %s", (out / _safe_name(meta.label)).as_posix())
    else:
        logger.info("Usando archivo desde cache: %s", meta.label)

    # leer
    df = _read_stata(raw) if meta.label.lower().endswith(".dta") else _read_excel_main_sheet(raw)
    df = _normalize_panel(df)

    # filtros opcionales
    if countries:
        cc = {c.strip().upper() for c in countries}
        df = df[df["iso"].str.upper().isin(cc) | df["country"].str.upper().isin(cc)]
    if keep_vars:
        keep = ["iso","country","year"] + [v for v in keep_vars if v in df.columns]
        missing = [v for v in (keep_vars) if v not in df.columns]
        if missing: logger.warning("Variables no encontradas: %s", ", ".join(missing))
        df = df[keep]
    if downcast:
        nums = df.select_dtypes(include=["float","int"]).columns
        df[nums] = df[nums].apply(pd.to_numeric, downcast="float")

    vista_figura, wide_panel = _build_views(df)
    pwt_main = df
    logger.info("Listo: pwt_main=%s×%s, vista_figura=%s×%s, wide_panel=%s×%s",
                *pwt_main.shape, *vista_figura.shape, *wide_panel.shape)
    return pwt_main, vista_figura, wide_panel

def export_csv(pwt_main: pd.DataFrame, vista_figura: pd.DataFrame, wide_panel: pd.DataFrame, out_dir: str | Path) -> None:
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    pwt_main.to_csv(out / "pwt_main.csv", index=False)
    vista_figura.to_csv(out / "pwt_view_figura.csv", index=False)
    wide_panel.to_csv(out / "pwt_view_wide.csv", index=False)
    logger.info("CSV guardados en: %s", out.as_posix())

# -------------------- Ejecución como script --------------------
if __name__ == "__main__":
    # Ajusta aquí lo que quieras exportar por defecto:
    pwt_main, vista_figura, wide_panel = load_pwt(out_dir="pwt_out", use_cache=True)
    export_csv(pwt_main, vista_figura, wide_panel, out_dir="pwt_out")

    # Deja las variables disponibles si ejecutas con `python -i pwt_loader_clean.py`
    import __main__
    __main__.pwt_main = pwt_main
    __main__.vista_figura = vista_figura
    __main__.wide_panel = wide_panel

    logger.info("OK ✓  Variables disponibles: pwt_main, vista_figura, wide_panel")
