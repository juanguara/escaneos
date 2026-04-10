# Instalacion En Windows

## Requisitos

- Windows 10 u 11
- Python 3.10 o superior
- Acceso a internet para instalar dependencias
- Una API key de OpenAI guardada para el uso de IA

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

## 5. Configurar La API Key

1. En la raiz del proyecto, crear un archivo llamado `.env`
2. Agregar una linea con la clave:

```env
OPENAI_API_KEY=tu_api_key_real
```

Si no se quiere usar IA por el momento, la app igual puede correr sin la clave, pero los campos manuscritos van a quedar vacios.

## 6. Preparar Carpetas

Crear o copiar estas carpetas:

- `escaneados`
- `salida`
- `fallados`

`escaneados` debe contener las imagenes a procesar.

Tambien puede contener archivos PDF multipagina. En ese caso la app procesa una pagina por documento.

## 7. Ejecutar La App

Desde la raiz del proyecto, con el entorno virtual activado:

```powershell
scan-indexer --input-dir .\escaneados --output-dir .\salida --failed-dir .\fallados --model gpt-5.4-mini --copy-originals --verbose
```

Si el comando `scan-indexer` no funciona, usar:

```powershell
python -m scan_indexer.cli --input-dir .\escaneados --output-dir .\salida --failed-dir .\fallados --model gpt-5.4-mini --copy-originals --verbose
```

## 8. Archivos De Salida

Al terminar, la app genera:

- `salida\results.json`
- `salida\results.xlsx`

Ahora cada corrida queda dentro de una subcarpeta nueva en `salida`, por ejemplo:

- `salida\2026-04-09_001\results.json`
- `salida\2026-04-09_001\results.xlsx`

Si se procesan PDFs, cada pagina queda como un registro separado con su numero de pagina.

Y copia a:

- `salida\` los archivos procesados correctamente si se uso `--copy-originals`
- `fallados\` los archivos con error de procesamiento

## 8.1 Reprocesar Solo El `results.json`

Si ya existe `salida\results.json` y queres rehacer solamente la parte de base de datos, sin volver a leer documentos:

```powershell
scan-indexer --output-dir .\salida --failed-dir .\fallados --results-json-only --dry-run-db-updates --verbose
```

O para aplicar updates reales:

```powershell
scan-indexer --output-dir .\salida --failed-dir .\fallados --results-json-only --apply-db-updates --verbose
```

En este modo no hace OCR ni reprocesa PDFs o imagenes.
Toma el `results.json` más reciente dentro de `salida`.

## 9. Problemas Comunes

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

## 10. Recomendacion Para Produccion

En la version final con escaneos, conviene:

- usar archivos escaneados rectos
- mantener siempre el mismo formato de hoja
- separar una carpeta para pruebas
- validar los primeros lotes revisando `results.json` y `results.xlsx`
