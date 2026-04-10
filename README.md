# Scan Indexer MVP

MVP local para recorrer una carpeta de documentos escaneados, extraer:

- Dos códigos de barras con librería local.
- Aclaración, documento, vínculo, BP y referencias con IA sobre recortes.
- Soporte para imágenes sueltas y PDFs multipágina, una página por documento.
- Un `results.xlsx` y un `results.json` en la carpeta de salida.
- Cada corrida queda en su propia subcarpeta dentro de `output-dir`, con formato `YYYY-MM-DD_###`.

## Requisitos

- Python 3.9+
- `OPENAI_API_KEY` configurada si querés usar reconocimiento manuscrito con IA.

## Instalación

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Configuración de API

Podés crear un archivo `.env` en la raíz del proyecto con:

```bash
OPENAI_API_KEY=tu_api_key_aqui
PPOSTAL_DB_HOST=200.58.127.105
PPOSTAL_DB_PORT=5432
PPOSTAL_DB_NAME=ppostal
PPOSTAL_DB_USER=postgres
PPOSTAL_DB_PASSWORD=tu_password_postgres
```

## Uso

```bash
scan-indexer \
  --input-dir ./escaneados \
  --output-dir ./salida \
  --failed-dir ./fallados \
  --model gpt-5.4-mini \
  --copy-originals
```

## Aplicar Actualización En PostgreSQL

Para además pasar las órdenes a estado `Cumplida` y registrar `order_tracking`:

```bash
scan-indexer \
  --input-dir ./escaneados \
  --output-dir ./salida \
  --failed-dir ./fallados \
  --model gpt-5.4-mini \
  --copy-originals \
  --apply-db-updates
```

## Simulación De Actualización En PostgreSQL

Para generar un plan de transacciones sin escribir en la base:

```bash
scan-indexer \
  --input-dir ./escaneados \
  --output-dir ./salida \
  --failed-dir ./fallados \
  --model gpt-5.4-mini \
  --copy-originals \
  --dry-run-db-updates
```

Esto genera además:

- `salida/YYYY-MM-DD_###/db_dry_run.json`

## Reprocesar Solo `results.json`

Si ya tenés `salida/results.json` y querés volver a validar en base, simular updates o aplicar updates sin releer documentos:

```bash
scan-indexer \
  --output-dir ./salida \
  --failed-dir ./fallados \
  --results-json-only \
  --dry-run-db-updates
```

En este modo:

- no recorre `--input-dir`
- no vuelve a procesar imágenes ni PDFs
- reutiliza el `results.json` más reciente dentro de `output-dir`
- vuelve a generar `results.json`, `results.xlsx` y, si corresponde, `db_dry_run.json`

## Modo local sin IA

```bash
scan-indexer \
  --input-dir ./escaneados \
  --output-dir ./salida \
  --failed-dir ./fallados \
  --skip-ai
```

## Notas

- El pipeline intenta enderezar la hoja, leer barcodes y luego recorta zonas fijas del formulario para la IA.
- Si entra un PDF, cada página se procesa como un documento independiente y la salida agrega la columna `pagina`.
- Cada ejecución crea una subcarpeta nueva en `output-dir`, por ejemplo `salida/2026-04-09_001`.
- Si hay credenciales PostgreSQL, valida `barcode_1` contra `purchase_order.barcode`; si no existe, el documento queda en `fallados`.
- Los updates en `purchase_order_status` y `order_tracking` solo se ejecutan si se usa `--apply-db-updates`.
- Si se usa `--dry-run-db-updates`, no escribe en la base y genera el detalle de SQL esperado en `db_dry_run.json`.
- Si falla la extracción de un documento, se copia el original a la carpeta de fallados y el motivo queda en `results.json` y `results.xlsx`.
- En este MVP los originales no se mueven; opcionalmente se copian a la salida con `--copy-originals`.
