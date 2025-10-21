# Guía de Contribución

¡Gracias por tu interés en **ingesta-datos-publicos**! Este proyecto reúne *loaders* y utilidades
para obtener y estandarizar **datos públicos** (p. ej., BanRep, PWT) en CSV/Parquet y DataFrames.

> Al enviar un PR aceptas que tu contribución se licencie bajo **MIT** (ver `LICENSE`).

---

## 1) Cómo puedo ayudar

- **Reporta bugs** : abre un *issue* con el template “Bug report”.
- **Propón mejoras** : abre un *issue* “Feature request”.
- **Envía PRs pequeños y enfocados** : una idea por PR.

### Buenas primeras tareas
- Añadir un *loader* sencillo de una nueva fuente pública.
- Mejorar documentación (`README`, docstrings) o tipado.
- Añadir pruebas para casos borde (fechas, tipos, NaN, encoding).

---

## 2) Entorno de desarrollo

```bash
git clone <tu-fork-o-este-repo>.git
cd ingesta-datos-publicos
make install              # crea .venv e instala deps (pip, ruff, pytest)
cp .env.example .env      # si necesitas tokens (no subas .env)
```

### Atajos útiles
```bash
make pwt                  # ejecuta src/pwt/pwt_loader_clean.py
make banrep               # ejecuta {banrep_script or 'src/banrep/<tu_script>.py'} con defaults
make lint                 # ruff
make test                 # pytest
```

> **Datos grandes**: no subas CSV/Parquet. Están ignorados en `.gitignore` (usa `tests/fixtures/`).

---

## 3) Estilo y calidad

- **Python ≥ 3.10**. Mantén compatibilidad.
- **Ruff** para *linting* (`make lint`). Puedes usar `ruff --fix` para autocorrecciones.
- **Logging** con `logging` de stdlib (niveles: INFO para “qué hago”, DEBUG para detalles).
- **Tipado** sugerido (type hints) en funciones públicas.
- **Docstrings** (estilo Google o NumPy) en funciones/CLI.

Ejemplo breve:
```python
def load_fuente(out_dir: str = "out", use_cache: bool = True) -> "pd.DataFrame":
    """Descarga y normaliza la fuente X.

    Args:
        out_dir: Carpeta de salida para archivos derivados.
        use_cache: Reusar descargas previas (si existen).

    Returns:
        DataFrame en formato *tidy* listo para análisis.
    """
```

---

## 4) Pruebas

- Ejecuta `make test` antes de enviar el PR.
- Coloca *fixtures* pequeños (≤50 KB) en `tests/fixtures/`.
- Evita dependencias externas en tests (usa *mocks* o *fixtures*).

Prueba mínima de *smoke* ya incluida en `tests/test_smoke.py`:
- Importa módulos principales (PWT/BanRep) y valida que exponen funciones o CLI.

---

## 5) Commits y ramas

- Crea ramas por funcionalidad: `feat/<corto-descriptivo>`, `fix/...`, `docs/...`.
- **Convencional (recomendado)**: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`.
- Squash o rebase antes del merge si el historial quedó ruidoso.

---

## 6) Añadir una nueva fuente de datos (checklist)

- [ ] Crear carpeta `src/<proveedor>/` con `loader` principal (`load_<proveedor>.py`).
- [ ] Exponer **API en Python**: `load_<proveedor>(out_dir: str = "...", use_cache: bool = True) -> DataFrame | tuple`.
- [ ] Exponer **CLI** (`if __name__ == "__main__": ...`) con parámetros básicos (`--out-dir`, `--log`, flags para CSV/Parquet).
- [ ] Normalizar a **tidy** y (opcional) **wide**; documentar claves y *dtypes*.
- [ ] Registrar **licencia/ToS** de la fuente (en el módulo o en `README`).
- [ ] Manejar **errores/transientes** (reintentos, timeouts) y **encoding/locale**.
- [ ] Añadir **tests** (cobertura de casos borde) + *fixtures* pequeñas.
- [ ] Actualizar `README` (sección “Fuentes incluidas”) y ejemplos de uso.
- [ ] Ejecutar `make lint` y `make test` localmente.
- [ ] Abrir PR enlazando al *issue*.

---

## 7) Proceso de Pull Request

1. Asegúrate de que haya un *issue* asociado (bug/mejora).
2. Actualiza docs y *changelog* del PR (en la descripción).
3. Pasa CI (lint + tests). El flujo `ci.yml` lo ejecuta automáticamente.
4. Pide 1 review. Se prioriza **claridad** y **seguridad de datos** (no exponer tokens/PII).

---

## 8) Seguridad y secretos

- Nunca subas secretos (tokens, cookies). Usa `.env` y variables de entorno.
- Si detectas un problema sensible, **no abras un issue público**; contacta por privado.

---

## 9) Código de conducta

Nos regimos por principios de **respeto, colaboración y diversidad**. No se tolera acoso ni conductas discriminatorias. Ver `CODE_OF_CONDUCT.md`.

---

## 10) Contacto

Abre un *issue* o menciona a los *maintainers* en un PR. ¡Gracias por contribuir!
