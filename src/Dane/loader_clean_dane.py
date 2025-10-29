
"""

Datos Dane


En contrucción 

Falta solucionar el captcha y/0 la consecución del Token

"""

from __future__ import annotations
import argparse
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests

BASE = "https://microdatos.dane.gov.co/index.php"
UA = "DANE-NADA-Extractor/1.0 (+github.com/your-org)"
LOG = logging.getLogger("nada")


# ----------------------------- utilidades ------------------------------------
def _get(d: Dict, *keys, default=None):
    """Acceso tolerante a llaves anidadas: _get(d, 'a','b','c', default=None)."""
    cur = d
    try:
        for k in keys:
            if cur is None:
                return default
            if isinstance(cur, dict):
                cur = cur.get(k, default)
            else:
                return default
        return cur if cur is not None else default
    except Exception:
        return default


def _norm_list(x) -> List:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _normalize_categories(cat) -> Tuple[Optional[int], Optional[str]]:
    """
    Devuelve (#categorías, JSON compacto [{"value":..,"label":..},...]) si existen.
    NADA puede exponer 'var_catgry' (dict->list) o 'categories' (list).
    """
    if cat is None:
        return None, None
    try:
        if isinstance(cat, dict) and isinstance(cat.get("var_catgry"), list):
            src = cat["var_catgry"]
        elif isinstance(cat, list):
            src = cat
        else:
            return None, None
        items = []
        for c in src:
            val = c.get("value") or c.get("cat_val")
            lab = c.get("label") or c.get("cat_lab")
            items.append({"value": val, "label": lab})
        if not items:
            return None, None
        return len(items), json.dumps(items, ensure_ascii=False)
    except Exception:
        return None, None


def _retry_get(session: requests.Session, url: str, **kwargs) -> requests.Response:
    """GET con reintentos simples (429/5xx)."""
    backoff = 1.0
    for attempt in range(5):
        r = session.get(url, timeout=kwargs.get("timeout", 60), params=kwargs.get("params"))
        if r.status_code < 400:
            return r
        if r.status_code in (429, 500, 502, 503, 504):
            LOG.warning("GET %s -> %s, retry in %.1fs", url, r.status_code, backoff)
            time.sleep(backoff)
            backoff *= 2
            continue
        r.raise_for_status()
    r.raise_for_status()
    return r  # no llega


# ----------------------------- cliente NADA ----------------------------------
@dataclass
class NadaClient:
    base: str = BASE
    user_agent: str = UA

    def __post_init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})

    def search_catalog(
        self,
        q: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        page_size: int = 100,
        max_pages: int = 10,
        limit_studies: Optional[int] = None,
    ) -> List[int]:
        """
        Devuelve lista de study_ids desde /api/catalog/search (orden proddate desc).
        """
        ids: List[int] = []
        offset = 0
        for page in range(max_pages):
            params = {"ps": page_size, "offset": offset, "sort_by": "proddate", "sort_order": "desc"}
            if q:
                params["q"] = q
            if date_from:
                params["from"] = date_from
            if date_to:
                params["to"] = date_to

            url = f"{self.base}/api/catalog/search"
            r = _retry_get(self.session, url, params=params, timeout=60)
            data = r.json().get("result", {})
            rows = data.get("rows", []) or []
            LOG.info("search page=%s offset=%s -> %s estudios", page + 1, offset, len(rows))
            if not rows:
                break
            ids.extend([row.get("id") for row in rows if row.get("id") is not None])

            if limit_studies and len(ids) >= limit_studies:
                return ids[:limit_studies]

            offset += page_size
            if len(rows) < page_size:
                break
        return ids

    def export_metadata(self, study_id: int) -> Dict[str, Any]:
        """/metadata/export/{study_id}/json"""
        url = f"{self.base}/metadata/export/{study_id}/json"
        r = _retry_get(self.session, url, timeout=120)
        return r.json()


# ------------------------ parseo: estudio / variables ------------------------
def parse_study(md: Dict[str, Any], study_id: int) -> Dict[str, Any]:
    """Fila nivel estudio, robusta a variantes NADA."""
    sd = md.get("study_desc", {}) or {}
    da = md.get("data_access", {}) or sd.get("data_access", {}) or {}
    title = md.get("title") or _get(sd, "title_statement", "title", default="")
    keywords = [k.get("keyword") or k.get("value") for k in _norm_list(_get(sd, "keywords", "keyword"))]
    topics = [t.get("topic") or t.get("value") for t in _norm_list(_get(sd, "study_info", "topics", "topic"))]
    producers = [p.get("name") for p in _norm_list(_get(sd, "production_statement", "producers", "producer"))]
    funding = [f.get("name") or f.get("agency") for f in _norm_list(_get(sd, "funding", "agency"))]

    return {
        "study_id": study_id,
        "idno": md.get("idno") or _get(sd, "title_statement", "idno"),
        "title": title,
        "sub_title": _get(sd, "title_statement", "sub_title"),
        "nation": _get(sd, "study_info", "nation", "name"),
        "abbreviation": _get(sd, "title_statement", "alternate_title"),
        "year_start": _get(sd, "study_info", "dates", "start"),
        "year_end": _get(sd, "study_info", "dates", "end"),
        "proddate": md.get("proddate") or _get(sd, "production_statement", "prod_date"),
        "repositoryid": md.get("repositoryid"),
        "version": md.get("version") or _get(sd, "version_statement", "version"),
        "abstract": _get(sd, "study_info", "abstract"),
        "data_kind": _get(sd, "study_info", "data_kind"),
        "geog_coverage": _get(sd, "study_info", "geog_coverage"),
        "universe_study": _get(sd, "study_info", "universe"),
        "keywords": "; ".join([x for x in keywords if x]),
        "topics": "; ".join([x for x in topics if x]),
        "producers": "; ".join([x for x in producers if x]),
        "funding": "; ".join([x for x in funding if x]),
        # Acceso / licencia (si existen)
        "access_policy": da.get("access_policy") or da.get("dataset_availability"),
        "confidentiality": da.get("confidentiality") or _get(sd, "data_access", "confidentiality"),
        "conditions": da.get("conditions"),
        "disclaimer": da.get("disclaimer"),
        "citation_requirement": da.get("cit_req") or da.get("citation_requirements"),
        # DOI / enlaces frecuentes
        "doi": md.get("doi") or _get(sd, "citation", "titlstat", "doi"),
    }


def parse_variables(md: Dict[str, Any], study_id: int) -> List[Dict[str, Any]]:
    """Filas nivel variable. Incluye: universe, preguntas (pre/lit/post), formato, categorías, archivo, etc."""
    title = md.get("title") or _get(md, "study_desc", "title_statement", "title", default="")
    files = md.get("files", []) if isinstance(md.get("files"), list) else []
    file_map = {f.get("file_id"): f.get("file_name") for f in files if f.get("file_id") is not None}

    variables = md.get("variables") or []
    if not isinstance(variables, list):
        return []

    out: List[Dict[str, Any]] = []
    for v in variables:
        vname = (v.get("name") or v.get("var_name") or "").strip()
        if not vname:
            continue

        vlabel = (v.get("labl") or v.get("var_label") or v.get("label") or "").strip()
        vtype = v.get("var_format") or v.get("type") or v.get("vartype") or ""
        vdcml = v.get("var_dcml")
        vintr = v.get("var_intrvl")
        vwgt  = v.get("var_wgt")

        # Universo & preguntas (llaves pueden variar)
        vuniv = v.get("universe") or v.get("universe_txt") or ""
        preq  = _get(v, "qstn", "qstn_preqtext") or v.get("qstn_preqtext") or ""
        qlit  = _get(v, "qstn", "qstn_qstnlit") or v.get("qstn_qstnlit") or ""
        postq = _get(v, "qstn", "qstn_postqtxt") or v.get("qstn_postqtxt") or ""
        notes = v.get("notes") or v.get("txt") or ""

        # Archivo(s) asociados
        vfiles = v.get("files") or v.get("file_id")
        if isinstance(vfiles, list):
            v_file_ids = [int(x) for x in vfiles if str(x).isdigit()]
        elif isinstance(vfiles, int):
            v_file_ids = [vfiles]
        else:
            v_file_ids = []

        ncat, cat_json = _normalize_categories(v.get("var_catgry") or v.get("categories"))

        out.append({
            "study_id": study_id,
            "study_title": title,
            "var_id": v.get("vid") or v.get("uid") or v.get("id"),
            "var_name": vname,
            "var_label": vlabel,
            "var_type": vtype,
            "var_dcml": vdcml,
            "var_intrvl": vintr,
            "var_wgt": vwgt,
            "file_ids": ";".join(str(x) for x in v_file_ids) if v_file_ids else None,
            "file_names": ";".join(file_map.get(fid, "") for fid in v_file_ids) if v_file_ids else None,
            "universe": vuniv,
            "q_preqtext": preq,
            "q_qstnlit": qlit,
            "q_postqtxt": postq,
            "notes": notes,
            "n_categories": ncat,
            "categories_json": cat_json,
        })
    return out


# --------------------------- pipeline principal ------------------------------
def run_extraction(
    q: Optional[str] = None,
    sid_list: Optional[List[int]] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit_studies: int = 25,
    page_size: int = 100,
    max_pages: int = 10,
    # Filtros de variables
    var_names: Optional[List[str]] = None,
    var_name_regex: Optional[str] = None,
    var_label_regex: Optional[str] = None,
    file_ids_filter: Optional[List[int]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Ejecuta la extracción y devuelve (df_vars, df_studies).

    - Puedes pasar study_ids en sid_list o resolverlos por búsqueda con q.
    - Filtros de variables:
        var_names       -> lista de nombres exactos (case-insensitive)
        var_name_regex  -> regex aplicada al nombre
        var_label_regex -> regex aplicada a la etiqueta/label
        file_ids_filter -> limita a variables cuyos file_id intersecten con la lista
    """
    client = NadaClient()

    if sid_list:
        study_ids = [int(x) for x in sid_list]
    else:
        if not q:
            raise ValueError("Debes proporcionar --sid o --q (texto de búsqueda).")
        study_ids = client.search_catalog(
            q=q, date_from=date_from, date_to=date_to,
            page_size=page_size, max_pages=max_pages, limit_studies=limit_studies
        )
    if not study_ids:
        raise RuntimeError("No se encontraron estudios con los parámetros dados.")

    all_vars: List[Dict[str, Any]] = []
    all_studies: List[Dict[str, Any]] = []

    name_set = {s.lower() for s in var_names} if var_names else None
    name_rx = re.compile(var_name_regex, flags=re.IGNORECASE) if var_name_regex else None
    lab_rx  = re.compile(var_label_regex,  flags=re.IGNORECASE) if var_label_regex  else None
    file_set = set(file_ids_filter) if file_ids_filter else None

    for sid in study_ids:
        md = client.export_metadata(sid)

        # Estudio
        all_studies.append(parse_study(md, sid))

        # Variables (sin filtrar)
        rows = parse_variables(md, sid)

        # Aplicar filtros (si los hay)
        out_rows: List[Dict[str, Any]] = []
        for r in rows:
            ok = True
            if name_set is not None:
                ok = (r["var_name"] or "").lower() in name_set
            if ok and name_rx:
                ok = bool(name_rx.search(r["var_name"] or ""))
            if ok and lab_rx:
                ok = bool(lab_rx.search(r["var_label"] or ""))
            if ok and file_set:
                # file_ids es string "1;3;5" o None
                ids = [int(x) for x in (r.get("file_ids") or "").split(";") if x.isdigit()]
                ok = bool(set(ids) & file_set)
            if ok:
                out_rows.append(r)

        all_vars.extend(out_rows)

    # DataFrames
    df_vars = pd.DataFrame(all_vars, columns=[
        "study_id","study_title","var_id","var_name","var_label","var_type",
        "var_dcml","var_intrvl","var_wgt","file_ids","file_names",
        "universe","q_preqtext","q_qstnlit","q_postqtxt","notes",
        "n_categories","categories_json"
    ])
    df_studies = pd.DataFrame(all_studies, columns=[
        "study_id","idno","title","sub_title","nation","abbreviation",
        "year_start","year_end","proddate","repositoryid","version",
        "abstract","data_kind","geog_coverage","universe_study",
        "keywords","topics","producers","funding",
        "access_policy","confidentiality","conditions","disclaimer",
        "citation_requirement","doi"
    ])
    return df_vars, df_studies


# ---------------------------------- CLI --------------------------------------
def _main():
    ap = argparse.ArgumentParser(description="Extractor NADA DANE (variables + estudios)")
    sel = ap.add_mutually_exclusive_group(required=True)
    sel.add_argument("--q", type=str, help="Texto de búsqueda (catálogo)")
    sel.add_argument("--sid", type=str, help="IDs de estudio separados por coma")

    ap.add_argument("--from-date", type=str, help="Fecha inicial (YYYY-MM-DD)")
    ap.add_argument("--to-date", type=str, help="Fecha final (YYYY-MM-DD)")
    ap.add_argument("--limit-studies", type=int, default=25)
    ap.add_argument("--page-size", type=int, default=100)
    ap.add_argument("--max-pages", type=int, default=10)

    # Filtros de variables
    ap.add_argument("--vars", type=str, help="Nombres exactos de variables (coma)")
    ap.add_argument("--var-name-regex", type=str, help="Regex en nombre de variable")
    ap.add_argument("--var-label-regex", type=str, help="Regex en etiqueta/label")
    ap.add_argument("--file-ids", type=str, help="file_id del/los archivos (coma)")

    # Salida
    ap.add_argument("--out-vars", type=str, default="./out/vars.parquet")
    ap.add_argument("--out-studies", type=str, default="./out/studies.parquet")
    fmt = ap.add_mutually_exclusive_group()
    fmt.add_argument("--csv", action="store_true", help="Guardar en CSV")
    fmt.add_argument("--parquet", action="store_true", help="Guardar en Parquet (default)")

    ap.add_argument("--log", type=str, default="INFO", help="DEBUG, INFO, WARNING, ERROR")
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    sid_list = [int(x.strip()) for x in args.sid.split(",")] if args.sid else None
    var_names = [x.strip() for x in args.vars.split(",")] if args.vars else None
    file_ids  = [int(x.strip()) for x in args.file_ids.split(",")] if args.file_ids else None

    df_vars, df_studies = run_extraction(
        q=args.q,
        sid_list=sid_list,
        date_from=args.from_date,
        date_to=args.to_date,
        limit_studies=args.limit_studies,
        page_size=args.page_size,
        max_pages=args.max_pages,
        var_names=var_names,
        var_name_regex=args.var_name_regex,
        var_label_regex=args.var_label_regex,
        file_ids_filter=file_ids,
    )

    # Guardar
    if args.csv or (not args.parquet and (args.out_vars.endswith(".csv") or args.out_studies.endswith(".csv"))):
        df_vars.to_csv(args.out_vars if args.out_vars.endswith(".csv") else args.out_vars.replace(".parquet", ".csv"), index=False)
        df_studies.to_csv(args.out_studies if args.out_studies.endswith(".csv") else args.out_studies.replace(".parquet", ".csv"), index=False)
    else:
        df_vars.to_parquet(args.out_vars if args.out_vars.endswith(".parquet") else args.out_vars.replace(".csv", ".parquet"), index=False)
        df_studies.to_parquet(args.out_studies if args.out_studies.endswith(".parquet") else args.out_studies.replace(".csv", ".parquet"), index=False)

    LOG.info("Listo: vars=%s (%d filas) | studies=%s (%d filas)",
             args.out_vars, len(df_vars), args.out_studies, len(df_studies))


if __name__ == "__main__":
    _main()
