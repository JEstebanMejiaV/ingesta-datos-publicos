# ingesta-datos-publicos

Colección de **loaders y utilidades en Python** para obtener y estandarizar datos públicos/abiertos con enfoque reproducible (CSV/Parquet y dataframes listos).

## Fuentes incluidas (hasta ahora)

- **DANE – Catálogo NADA (microdatos / metadatos)** → explorador/loader de metadatos y diccionarios de variables con filtros por texto, fechas y regex sobre nombres/etiquetas. Archivo: `src/Dane/loader_clean_dane.py`.
- **Banco de la República (BanRep)** → consolidador de CSVs (uno por flow) en formatos **largo (tidy)** y **ancho** con tipado seguro para Parquet. Archivo: `src/banrep/banrep_consolidate_v3.py`.
- **Penn World Table (PWT 11.x)** → loader por DOI desde Dataverse (`doi:10.34894/FABVLR`), genera panel tidy y vistas wide. Archivo: `src/pwt/pwt_loader_clean.py`.

> ⚠️ **Notas rápidas**
> - DANE/NADA: este loader trabaja con **metadatos y diccionarios** expuestos por el catálogo. La descarga de **microdatos** puede requerir autenticación/licencia (captcha/cookies no automatizadas).
> - PWT se descarga desde Dataverse; puedes usar token si lo necesitas (ver `.env.example`).

---

## Requisitos

- Python 3.10+
- `pip install -r requirements.txt`

---

## Uso rápido

### 1) DANE / NADA (metadatos + variables)

**Como script** (explora catálogo, filtra y exporta a Parquet/CSV):

```bash
python src/Dane/loader_clean_dane.py   --q "ingreso"   --from-date 2018-01-01   --to-date 2025-12-31   --var-name-regex "(ingres|salari|remuner)"   --out-vars ./out/vars.parquet   --out-studies ./out/studies.parquet   --log INFO
```

**Por IDs específicos del catálogo**:

```bash
python src/Dane/loader_clean_dane.py   --sid 805,1234,5678   --vars "ingreso_laboral,ocupacion"   --csv   --out-vars ./out/vars.csv   --out-studies ./out/studies.csv
```

**Parámetros útiles** (`--help` para ver todos):

- Selección de estudios: `--q` *o* `--sid` (mutuamente excluyentes)
- Ventana temporal del catálogo: `--from-date`, `--to-date`
- Paginación: `--limit-studies`, `--page-size`, `--max-pages`
- Filtros de variables: `--vars`, `--var-name-regex`, `--var-label-regex`, `--file-ids`
- Formato de salida: `--parquet` (default) o `--csv`
- Logging: `--log DEBUG|INFO|WARNING|ERROR`

**Esquemas de salida (columnas)**

- **`studies`**:  
  `study_id, idno, title, sub_title, nation, abbreviation, year_start, year_end, proddate, repositoryid, version, abstract, data_kind, geog_coverage, universe_study, keywords, topics, producers, funding, access_policy, confidentiality, conditions, disclaimer, citation_requirement, doi`

- **`vars`**:  
  `study_id, study_title, var_id, var_name, var_label, var_type, var_dcml, var_intrvl, var_wgt, file_ids, file_names, universe, q_preqtext, q_qstnlit, q_postqtxt, notes, n_categories, categories_json`

> **Tip operativo**: comienza con `--q` para identificar `study_id` y luego cierra el set con `--sid`. Usa regex en `--var-label-regex` para acotar temas (“educación”, “hogar”, etc.).

---

### 2) BanRep – consolidación de CSVs

Coloca tus CSVs (uno por flow) bajo `banrep_output/data/*.csv` y luego ejecuta:

```bash
python src/banrep/banrep_consolidate_v3.py   --out-dir banrep_output   --flows ALL   --log INFO
```

**Parámetros**:
- `--out-dir`: raíz con `/data` (CSVs de entrada) y `/catalog` (salidas)
- `--flows`: `ALL` (default) o lista separada por comas (`DF_TRM_DAILY_HIST,DF_IPC_MENSUAL,...`)
- `--save-parquet-long`, `--save-parquet-wide` (o CSVs equivalentes)

**Uso programático**:

```python
from src.banrep.banrep_consolidate_v3 import consolidate
df_long, df_wide = consolidate(out_dir="banrep_output", flows="ALL")
```

---

### 3) Penn World Table (PWT 11.x / Dataverse)

**Como script**:

```bash
python src/pwt/pwt_loader_clean.py
```

**Como librería**:

```python
from src.pwt.pwt_loader_clean import load_pwt, export_csv
pwt_main, vista_figura, wide_panel = load_pwt(out_dir="pwt_out", use_cache=True)
export_csv(pwt_main, vista_figura, wide_panel, out_dir="pwt_out")
```

---

## Variables de entorno

Crea `.env` (basado en `.env.example`) si necesitas tokens/cookies:

- `DATAVERSE_API_TOKEN`: token opcional para Dataverse (PWT).
- `AUTH_COOKIES`: (opcional) cookies de sesión para endpoints restringidos del catálogo DANE/NADA cuando apliquen.

> **Seguridad**: no subas `.env` ni cookies al repo.

---

## Estructura del repositorio

```
ingesta-datos-publicos/
├─ src/
│  ├─ Dane/
│  │  └─ loader_clean_dane.py
│  ├─ banrep/
│  │  └─ banrep_consolidate_v3.py
│  └─ pwt/
│     └─ pwt_loader_clean.py
├─ out/                        # salidas DANE (vars/studies) sugeridas
├─ banrep_output/              # entrada/salida BanRep (data/catalog)
├─ pwt_out/                    # salidas PWT
├─ data/                       # (opcional) datos locales temporales
├─ .env.example
├─ .gitignore
├─ Makefile
├─ pyproject.toml
├─ requirements.txt
├─ LICENSE
└─ README.md
```

---

## Makefile (atajos)

```bash
make install     # crea venv .venv e instala dependencias
make dane        # ejecuta DANE/NADA -> out/
make banrep      # consolida CSVs en banrep_output/
make pwt         # ejecuta loader de PWT -> pwt_out/
make lint        # ruff
make test        # pytest (placeholder por ahora)
```

---

## Roadmap breve

- Loader BanRep “crudo” (descarga directa por flows → `banrep_output/data`).
- World Bank / IMF / OECD loaders.
- Empaquetar (`pip install -e .`) y tests básicos.

---

## Notas legales

- Revisa términos de uso de cada fuente (DANE/NADA, PWT, BanRep, etc.).
- Este repo es **educativo**; no asume responsabilidad por el uso de los datos.

---

## Licencia

MIT © 2025 Juan Esteban Mejia
