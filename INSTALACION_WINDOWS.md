# Instalacion En Windows

## Requisitos

- Windows 10 u 11
- Python 3.10 o superior
- Acceso a internet para instalar dependencias
- Una API key de OpenAI si se quiere usar IA
- Credenciales de PostgreSQL si se quiere validar o impactar en base
- Opcional: Tesseract OCR para mejorar OCR local en Windows

## 1. Instalar Python

1. Descargar Python desde:
   https://www.python.org/downloads/windows/
2. Ejecutar el instalador.
3. Marcar la opcion `Add Python to PATH`.
4. Completar la instalacion.

## 2. Copiar El Proyecto

1. Copiar la carpeta del proyecto a la maquina Windows.
2. Abrir una terminal:
   - `PowerShell` recomendado
   - o `CMD`
3. Ir a la carpeta del proyecto. Ejemplo:

```powershell
cd C:\Escaneos
```

## 3. Crear El Entorno Virtual

En PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Si PowerShell bloquea la activacion, ejecutar:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

En CMD:

```cmd
python -m venv .venv
.\.venv\Scripts\activate.bat
```

## 4. Instalar Dependencias

Con el entorno virtual activado:

```powershell
python -m pip install --upgrade pip
pip install -e .
```

## 4.1 Instalar Tesseract En Windows Opcional

Si queres mejorar el OCR local en Windows, instalar Tesseract OCR:

1. Descargar desde:
   https://github.com/UB-Mannheim/tesseract/wiki
2. Instalarlo normalmente.
3. Durante la instalacion, dejar habilitada la opcion para agregarlo al `PATH` si aparece.
4. Cerrar y volver a abrir la terminal.

Para verificar:

```powershell
tesseract --version
```

## 5. Configurar `.env`

1. En la raiz del proyecto, crear un archivo llamado `.env`
2. Agregar las credenciales necesarias:

```env
OPENAI_API_KEY=tu_api_key_real
PPOSTAL_DB_HOST=200.58.127.105
PPOSTAL_DB_PORT=5432
PPOSTAL_DB_NAME=ppostal
PPOSTAL_DB_USER=postgres
PPOSTAL_DB_PASSWORD=tu_password_postgres
```

Notas:

- Si no se quiere usar IA, la app puede correr sin `OPENAI_API_KEY`, pero los campos manuscritos pueden quedar vacios o incompletos.
- Si no se quiere validar o actualizar la base, se puede omitir la password de PostgreSQL.
- En Windows, si `tesseract` esta instalado, la app lo usa como OCR local auxiliar.
- Si `tesseract` no esta instalado, la app igualmente funciona y sigue con el resto del pipeline sin romperse.

## 6. Preparar Carpetas

Crear o copiar estas carpetas:

- `escaneados`
- `salida`
- `fallados`

`escaneados` puede contener:

- imagenes sueltas
- PDFs multipagina

En el caso de PDFs, la app procesa una pagina por documento.

## 7. Comandos Principales

### 7.1 Procesar Documentos

Comando:

```powershell
scan-indexer --input-dir .\escaneados --output-dir .\salida --failed-dir .\fallados --model gpt-5.4-mini --copy-originals --verbose
```

Significado:

- `--input-dir`
  Carpeta de entrada con imagenes o PDFs.
- `--output-dir`
  Carpeta raiz donde se guarda cada corrida en una subcarpeta nueva.
- `--failed-dir`
  Carpeta donde se copian los documentos que fallan.
- `--model`
  Modelo de OpenAI usado para reconocimiento manuscrito.
- `--copy-originals`
  Copia a la carpeta de salida los documentos procesados con nombre `barcode_1`.
- `--verbose`
  Muestra mas detalle en consola.

### 7.2 Procesar Sin IA

Comando:

```powershell
scan-indexer --input-dir .\escaneados --output-dir .\salida --failed-dir .\fallados --skip-ai --verbose
```

Significado:

- `--skip-ai`
  Omite la parte de IA. Solo intenta extraer barcodes y datos locales.

### 7.3 Procesar Sin Validar Base

Comando:

```powershell
scan-indexer --input-dir .\escaneados --output-dir .\salida --failed-dir .\fallados --skip-db-check --verbose
```

Significado:

- `--skip-db-check`
  No consulta PostgreSQL para validar `barcode_1`.

### 7.4 Simular El Impacto En La Base

Comando:

```powershell
scan-indexer --input-dir .\escaneados --output-dir .\salida --failed-dir .\fallados --model gpt-5.4-mini --copy-originals --dry-run-db-updates --verbose
```

Significado:

- `--dry-run-db-updates`
  No escribe en la base. Genera un archivo `db_dry_run.json` con las transacciones que ejecutaria.

### 7.5 Impactar La Base De Datos

Comando:

```powershell
scan-indexer --input-dir .\escaneados --output-dir .\salida --failed-dir .\fallados --model gpt-5.4-mini --copy-originals --apply-db-updates --verbose
```

Significado:

- `--apply-db-updates`
  Actualiza realmente `purchase_order_status` y `order_tracking` en PostgreSQL para los documentos validos.

### 7.6 Reprocesar Solo El `results.json`

Comando:

```powershell
scan-indexer --output-dir .\salida --failed-dir .\fallados --results-json-only --dry-run-db-updates --verbose
```

O para impacto real:

```powershell
scan-indexer --output-dir .\salida --failed-dir .\fallados --results-json-only --apply-db-updates --verbose
```

Significado:

- `--results-json-only`
  No relee documentos. Reutiliza el `results.json` mas reciente dentro de `salida`.

Este modo sirve para:

- volver a generar `results.xlsx`
- rehacer una simulacion
- impactar en base luego de revisar o corregir resultados

## 8. App Web De Revision

La app web local permite:

- elegir que carpeta de salida revisar
- ver la imagen completa del documento
- editar `BP`, `aclaracion`, `documento`, `fecha_entrega` y `vinculo`
- guardar automaticamente al navegar
- impactar un registro o todos los revisados

### 8.1 Levantar El Webserver

Comando:

```powershell
scan-indexer-review --output-dir .\salida
```

Opcionalmente se puede cambiar host o puerto:

```powershell
scan-indexer-review --output-dir .\salida --host 127.0.0.1 --port 8765
```

Significado:

- `--output-dir`
  Carpeta raiz donde estan las corridas para revisar.
- `--host`
  Direccion local donde escucha la app web.
- `--port`
  Puerto local de la app web.

### 8.2 Abrir La App Web

Una vez levantado el servidor, abrir en el navegador:

```text
http://127.0.0.1:8765
```

### 8.3 Flujo Recomendado

1. Correr extracción sin impacto real, por ejemplo con `--dry-run-db-updates`
2. Abrir la app web
3. Elegir la carpeta de salida a revisar
4. Corregir los campos necesarios
5. Guardar o navegar
   La app guarda automaticamente si se usa `Anterior` o `Siguiente`
6. Impactar cada registro o todos los revisados

## 9. Archivos De Salida

Cada corrida se guarda dentro de una subcarpeta nueva en `salida`, por ejemplo:

- `salida\2026-04-10_001\results.json`
- `salida\2026-04-10_001\results.xlsx`
- `salida\2026-04-10_001\db_dry_run.json`

Si se usa `--copy-originals`, tambien quedan dentro de esa misma carpeta:

- las imagenes procesadas correctamente
- renombradas con `barcode_1`

En `fallados\` quedan los documentos que no pudieron procesarse correctamente.

## 10. Si `scan-indexer` O `scan-indexer-review` No Funcionan

Usar estos comandos alternativos:

```powershell
python .\src\scan_indexer\pipeline.py --help
python .\src\scan_indexer\review_app.py --help
```

O bien:

```powershell
python -m scan_indexer.cli --input-dir .\escaneados --output-dir .\salida --failed-dir .\fallados --verbose
python -m scan_indexer.review_app --output-dir .\salida
```

## 11. Problemas Comunes

### Python no se reconoce

Instalar Python nuevamente y verificar que este marcada la opcion `Add Python to PATH`.

### No activa el entorno virtual

En PowerShell usar:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

### Error de API de OpenAI

Revisar:

- que el archivo `.env` exista
- que `OPENAI_API_KEY` este bien escrita
- que la cuenta de API tenga billing activo

### La IA no responde pero la app corre

Sin API key o sin credito, el sistema puede seguir con la parte local:

- barcode principal
- barcode secundario

Pero los campos manuscritos pueden quedar vacios o incompletos.

### Error al reprocesar desde `fallados`

Si se usa `fallados` como entrada y un archivo vuelve a fallar, la app ahora evita el choque de nombres agregando sufijos tipo:

- `archivo__retry1.jpg`
- `archivo__retry2.jpg`

## 12. Recomendacion Para Produccion

En la version final con escaneos, conviene:

- usar archivos escaneados rectos
- mantener siempre el mismo formato de hoja
- separar una carpeta para pruebas
- revisar los primeros lotes desde la app web antes de impactar en base
