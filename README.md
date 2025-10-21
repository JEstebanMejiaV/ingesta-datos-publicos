# ingesta-datos-publicos

Colección de **loaders y utilidades en Python** para obtener y estandarizar datos públicos/abiertos con enfoque reproducible (CSV/Parquet y dataframes listos).

## Fuentes incluidas (hasta ahora)
- **Banco de la República (BanRep)** → *consolidador de CSVs* (uno por flow) en formatos **largo (tidy)** y **ancho** con tipado seguro para Parquet. Archivo: `src/banrep/banrep_consolidate_v3.py`.
- **Penn World Table (PWT 11.x)** → *loader por DOI* desde Dataverse (`doi:10.34894/FABVLR`), genera panel tidy y vistas wide. Archivo: `src/pwt/pwt_loader_clean.py`.

> ⚠️ Nota: PWT se descarga desde Dataverse; puedes usar un token si lo necesitas (ver `.env.example`).

## Requisitos
- Python 3.10+
- `pip install -r requirements.txt`

## Uso rápido

### 1) Penn World Table (PWT 11.x / Dataverse)
**Como script** (descarga y deja CSVs en `pwt_out/`):  
```bash
python src/pwt/pwt_loader_clean.py
```

**Como librería** (y dataframe en memoria):  
```python
from src.pwt.pwt_loader_clean import load_pwt, export_csv

# Descarga/lee y devuelve tres vistas
pwt_main, vista_figura, wide_panel = load_pwt(out_dir="pwt_out", use_cache=True)

# Exportar CSVs si quieres
export_csv(pwt_main, vista_figura, wide_panel, out_dir="pwt_out")
```

### 2) BanRep – consolidación de CSVs
Coloca tus CSVs (uno por flow) bajo `banrep_output/data/*.csv` y luego ejecuta:

```bash
python src/banrep/banrep_consolidate_v3.py --out-dir banrep_output --flows ALL --log INFO
```

**Parámetros útiles** (`--help` para más):
- `--out-dir`: raíz con `/data` (CSVs de entrada) y `/catalog` (salidas).
- `--flows`: `ALL` (default) o lista separada por comas (`DF_TRM_DAILY_HIST,DF_IPC_MENSUAL,...`).
- `--save-parquet-long`, `--save-parquet-wide` (o CSVs equivalentes).

**Uso programático**:

```python
from src.banrep.banrep_consolidate_v3 import consolidate

df_long, df_wide = consolidate(out_dir="banrep_output", flows="ALL")
```

## Variables de entorno
Crea un archivo `.env` (basado en `.env.example`) en la raíz del repo si necesitas tokens:

- `DATAVERSE_API_TOKEN`: token opcional para Dataverse (PWT).

## Estructura del repositorio

```
ingesta-datos-publicos/
├─ src/
│  ├─ banrep/
│  │  └─ banrep_consolidate_v3.py
│  └─ pwt/
│     └─ pwt_loader_clean.py
├─ data/                        # (opcional) datos locales temporales
├─ .env.example
├─ .gitignore
├─ Makefile
├─ pyproject.toml
├─ requirements.txt
├─ LICENSE
└─ README.md
```

## Makefile (atajos)
```bash
make install     # crea venv .venv e instala dependencias
make pwt         # ejecuta loader de PWT -> pwt_out/
make banrep      # consolida CSVs en banrep_output/
make lint        # ruff
make test        # pytest (placeholder por ahora)
```

## Roadmap breve
- Loader BanRep “crudo” (descarga directa por flows → `banrep_output/data`).
- DANE microdatos (con manejo de cookies/licencias cuando aplique).
- World Bank / IMF / OECD loaders.
- Paquete minimal (`pip install -e .`) y pequeños tests.

## Notas legales
- Revisa términos de uso de cada fuente (PWT, BanRep, DANE, etc.).
- Este repo es **educativo**; no asume responsabilidad por el uso de los datos.

## Licencia
MIT © 2025 Juan Esteban Mejia
