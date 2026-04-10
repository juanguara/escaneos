from __future__ import annotations

import argparse
import json
import mimetypes
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel

from scan_indexer.pipeline import (
    DocumentResult,
    VALID_VINCULOS,
    get_db_connection_string,
    mark_order_as_complied,
    write_outputs,
    find_latest_results_dir,
)
import psycopg


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aplicacion local para revision visual de resultados.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Carpeta raiz de salidas.")
    parser.add_argument("--failed-dir", default=Path("fallados"), type=Path, help="Carpeta de fallados para buscar imagenes.")
    parser.add_argument("--host", default="127.0.0.1", help="Host para la app web local.")
    parser.add_argument("--port", default=8765, type=int, help="Puerto para la app web local.")
    return parser.parse_args()


def list_run_dirs(output_dir: Path) -> List[Path]:
    run_dirs = [path for path in output_dir.iterdir() if path.is_dir() and (path / "results.json").exists()]
    return sorted(run_dirs, key=lambda path: path.name, reverse=True)


def load_run_records(run_dir: Path) -> List[DocumentResult]:
    payload = json.loads((run_dir / "results.json").read_text(encoding="utf-8"))
    records = [DocumentResult.from_record(item) for item in payload]
    changed = False
    for record in records:
        if record.review_status is None:
            record.review_status = "pending"
            changed = True
        if record.db_applied is None:
            record.db_applied = record.order_update_status == "updated"
            changed = True
    if changed:
        save_run_records(run_dir, records)
    return records


def save_run_records(run_dir: Path, records: List[DocumentResult]) -> None:
    write_outputs(run_dir, records, planned_operations=None)


def resolve_image_path(run_dir: Path, record: DocumentResult, failed_dir: Optional[Path] = None) -> Optional[Path]:
    candidates: List[Path] = []
    if record.barcode_1:
        for extension in (".jpg", ".jpeg", ".png", ".webp", ".tif", ".tiff", ".bmp"):
            candidates.append(run_dir / f"{record.barcode_1}{extension}")
            if failed_dir is not None:
                candidates.append(failed_dir / f"{record.barcode_1}{extension}")
    if record.archivo:
        candidates.append(run_dir / record.archivo)
        if failed_dir is not None:
            candidates.append(failed_dir / record.archivo)

    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def run_summary(run_dir: Path, records: List[DocumentResult]) -> Dict[str, Any]:
    reviewed = len([record for record in records if record.review_status == "reviewed"])
    impacted = len([record for record in records if record.db_applied])
    return {
        "name": run_dir.name,
        "record_count": len(records),
        "reviewed_count": reviewed,
        "pending_count": len(records) - reviewed,
        "impacted_count": impacted,
    }


def record_payload(run_dir: Path, records: List[DocumentResult], index: int, failed_dir: Optional[Path] = None) -> Dict[str, Any]:
    if index < 0 or index >= len(records):
        raise HTTPException(status_code=404, detail="Registro fuera de rango.")

    record = records[index]
    image_path = resolve_image_path(run_dir, record, failed_dir)
    return {
        "index": index,
        "total": len(records),
        "record": record.to_record(),
        "image_url": None if image_path is None else f"/api/runs/{run_dir.name}/image/{index}",
        "summary": run_summary(run_dir, records),
    }


class RecordUpdate(BaseModel):
    bp: bool
    aclaracion: Optional[str] = None
    documento: Optional[str] = None
    fecha_entrega: Optional[str] = None
    vinculo: Optional[str] = None


def normalize_optional_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def apply_update_to_record(record: DocumentResult, payload: RecordUpdate) -> None:
    record.bp = "SI" if payload.bp else "NO"
    record.aclaracion = normalize_optional_text(payload.aclaracion)
    record.documento = normalize_optional_text(payload.documento)
    record.fecha_entrega = normalize_optional_text(payload.fecha_entrega)
    record.vinculo = None if payload.bp else normalize_optional_text(payload.vinculo)
    record.review_status = "reviewed"
    record.reviewed_at = datetime.now().isoformat(timespec="seconds")


def apply_record_to_database(record: DocumentResult) -> None:
    if record.purchase_order_id is None:
        raise ValueError("El registro no tiene purchase_order_id.")
    connection_string = get_db_connection_string()
    if not connection_string:
        raise ValueError("No hay credenciales de PostgreSQL configuradas.")
    with psycopg.connect(connection_string) as conn:
        mark_order_as_complied(conn, int(record.purchase_order_id), record.to_record())
    record.order_update_status = "updated"
    record.db_applied = True


HTML_PAGE = """<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Revision de Acuses</title>
  <style>
    :root { --bg:#f5f1e8; --panel:#fffdf8; --ink:#1f1b16; --muted:#6f6558; --accent:#0d6b63; --line:#d8d0c4; }
    body { margin:0; font-family: "Segoe UI", sans-serif; background:linear-gradient(180deg,#efe8d9,#f8f4ec); color:var(--ink); }
    .wrap { max-width: 1380px; margin: 0 auto; padding: 20px; }
    .topbar, .editor { background:var(--panel); border:1px solid var(--line); border-radius:18px; box-shadow:0 10px 30px rgba(0,0,0,.05); }
    .topbar { padding:16px 18px; display:flex; gap:14px; align-items:center; flex-wrap:wrap; }
    .topbar label { font-size:14px; color:var(--muted); display:block; margin-bottom:4px; }
    .topbar select, .topbar button, .editor input, .editor select { font:inherit; }
    .topbar select, .editor input, .editor select { border:1px solid var(--line); border-radius:10px; padding:10px 12px; background:#fff; min-width:180px; }
    .topbar button, .editor button { border:0; border-radius:12px; padding:10px 14px; background:var(--accent); color:#fff; cursor:pointer; }
    .topbar button.secondary, .editor button.secondary { background:#857868; }
    .topbar button:disabled, .editor button:disabled { opacity:.5; cursor:not-allowed; }
    .layout { display:grid; grid-template-columns: 1.4fr .9fr; gap:18px; margin-top:18px; }
    .viewerStack { display:flex; flex-direction:column; gap:14px; }
    .viewer { background:#fff; border:1px solid var(--line); border-radius:18px; padding:14px; min-height:52vh; display:flex; align-items:center; justify-content:center; }
    .viewer img { width:100%; height:auto; border-radius:12px; object-fit:contain; }
    .zoomPanel { background:#fff; border:1px solid var(--line); border-radius:18px; padding:14px; }
    .zoomHeader { display:flex; gap:12px; align-items:center; justify-content:space-between; margin-bottom:10px; flex-wrap:wrap; }
    .zoomCanvas { height:240px; border-radius:14px; overflow:hidden; border:1px solid var(--line); background:#f7f3eb center 78% / 160% no-repeat; }
    .editor { padding:18px; display:flex; flex-direction:column; gap:14px; }
    .meta { display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:10px; font-size:14px; }
    .meta div { padding:10px 12px; background:#faf6ef; border-radius:12px; }
    .field { display:flex; flex-direction:column; gap:6px; }
    .field label { font-size:13px; color:var(--muted); }
    .checkline { display:flex; align-items:center; gap:10px; }
    .actions { display:flex; gap:10px; flex-wrap:wrap; margin-top:8px; }
    .status { font-size:14px; color:var(--muted); min-height:20px; }
    @media (max-width: 980px) { .layout { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="topbar">
      <div>
        <label>Carpeta de salida</label>
        <select id="runSelect"></select>
      </div>
      <div>
        <label>Impacto en DB</label>
        <div class="checkline">
          <input id="applyOnSave" type="checkbox" checked>
          <span>Impactar al guardar y navegar</span>
        </div>
      </div>
      <button id="refreshRuns" class="secondary">Actualizar carpetas</button>
      <button id="applyAll">Impactar revisados pendientes</button>
      <div id="summaryText"></div>
    </div>
    <div class="layout">
      <div class="viewerStack">
        <div class="viewer"><img id="docImage" alt="Documento"></div>
        <div class="zoomPanel">
          <div class="zoomHeader">
            <strong>Zoom Manuscrita</strong>
            <label for="zoomRange">Aumento</label>
            <input id="zoomRange" type="range" min="100" max="260" step="10" value="160">
            <span id="zoomValue">160%</span>
          </div>
          <div id="zoomCanvas" class="zoomCanvas"></div>
        </div>
      </div>
      <div class="editor">
        <div class="meta">
          <div><strong>Archivo:</strong> <span id="metaArchivo"></span></div>
          <div><strong>Registro:</strong> <span id="metaPos"></span></div>
          <div><strong>Barcode 1:</strong> <span id="metaBarcode1"></span></div>
          <div><strong>Barcode 2:</strong> <span id="metaBarcode2"></span></div>
          <div><strong>Estado rev.:</strong> <span id="metaReview"></span></div>
          <div><strong>DB:</strong> <span id="metaDb"></span></div>
        </div>
        <div class="field">
          <label><input id="bp" type="checkbox"> BP</label>
        </div>
        <div class="field">
          <label for="aclaracion">Aclaracion</label>
          <input id="aclaracion" type="text">
        </div>
        <div class="field">
          <label for="documento">Documento</label>
          <input id="documento" type="text">
        </div>
        <div class="field">
          <label for="fecha_entrega">Fecha</label>
          <input id="fecha_entrega" type="text">
        </div>
        <div class="field">
          <label for="vinculo">Vinculo</label>
          <select id="vinculo"></select>
        </div>
        <div class="actions">
          <button id="prevBtn" class="secondary">Anterior</button>
          <button id="saveBtn" disabled>Guardar</button>
          <button id="nextBtn" class="secondary">Siguiente</button>
          <button id="applyCurrentBtn">Impactar este</button>
        </div>
        <div class="status" id="statusText"></div>
      </div>
    </div>
  </div>
  <script>
    const state = { run: null, index: 0, total: 0, dirty: false };
    const vinculos = ["", __VINCULOS__];
    const vinculoSelect = document.getElementById("vinculo");
    vinculos.forEach(v => {
      const option = document.createElement("option");
      option.value = v;
      option.textContent = v || "(vacio)";
      vinculoSelect.appendChild(option);
    });

    function setStatus(message) {
      document.getElementById("statusText").textContent = message || "";
    }

    function updateZoomPanel() {
      const zoom = document.getElementById("zoomRange").value;
      const imageUrl = document.getElementById("docImage").src;
      document.getElementById("zoomValue").textContent = zoom + "%";
      const zoomCanvas = document.getElementById("zoomCanvas");
      zoomCanvas.style.backgroundImage = imageUrl ? `url("${imageUrl}")` : "none";
      zoomCanvas.style.backgroundSize = zoom + "%";
    }

    function markDirty() {
      state.dirty = true;
      document.getElementById("saveBtn").disabled = false;
    }

    async function fetchJson(url, options) {
      const response = await fetch(url, options);
      if (!response.ok) {
        const payload = await response.json().catch(() => ({detail: "Error inesperado"}));
        throw new Error(payload.detail || "Error inesperado");
      }
      return response.json();
    }

    function payloadFromForm() {
      return {
        bp: document.getElementById("bp").checked,
        aclaracion: document.getElementById("aclaracion").value,
        documento: document.getElementById("documento").value,
        fecha_entrega: document.getElementById("fecha_entrega").value,
        vinculo: document.getElementById("vinculo").value,
      };
    }

    function fillForm(data) {
      const record = data.record;
      state.index = data.index;
      state.total = data.total;
      document.getElementById("docImage").src = data.image_url || "";
      updateZoomPanel();
      document.getElementById("metaArchivo").textContent = record.archivo + (record.pagina ? " pag " + record.pagina : "");
      document.getElementById("metaPos").textContent = (data.index + 1) + " / " + data.total;
      document.getElementById("metaBarcode1").textContent = record.barcode_1 || "";
      document.getElementById("metaBarcode2").textContent = record.barcode_2 || "";
      document.getElementById("metaReview").textContent = record.review_status || "pending";
      document.getElementById("metaDb").textContent = record.db_applied ? "impactado" : (record.order_update_status || "pendiente");
      document.getElementById("bp").checked = record.bp === "SI";
      document.getElementById("aclaracion").value = record.aclaracion || "";
      document.getElementById("documento").value = record.documento || "";
      document.getElementById("fecha_entrega").value = record.fecha_entrega || "";
      document.getElementById("vinculo").value = record.vinculo || "";
      document.getElementById("summaryText").textContent =
        "Pendientes: " + data.summary.pending_count +
        " | Revisados: " + data.summary.reviewed_count +
        " | Impactados: " + data.summary.impacted_count;
      state.dirty = false;
      document.getElementById("saveBtn").disabled = true;
      document.getElementById("prevBtn").disabled = data.index === 0;
      document.getElementById("nextBtn").disabled = data.index >= data.total - 1;
      if (document.getElementById("bp").checked) {
        document.getElementById("vinculo").value = "";
      }
    }

    async function loadRuns() {
      const data = await fetchJson("/api/runs");
      const select = document.getElementById("runSelect");
      select.innerHTML = "";
      data.runs.forEach((run, idx) => {
        const option = document.createElement("option");
        option.value = run.name;
        option.textContent = run.name + " (" + run.record_count + ")";
        if ((!state.run && idx === 0) || state.run === run.name) option.selected = true;
        select.appendChild(option);
      });
      if (!state.run && data.runs.length) state.run = data.runs[0].name;
      if (state.run) await loadRecord(state.run, 0);
    }

    async function loadRecord(runName, index) {
      state.run = runName;
      const data = await fetchJson(`/api/runs/${encodeURIComponent(runName)}/records/${index}`);
      fillForm(data);
      setStatus("");
    }

    async function saveCurrent() {
      if (!state.run) return;
      const payload = payloadFromForm();
      const data = await fetchJson(`/api/runs/${encodeURIComponent(state.run)}/records/${state.index}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      fillForm(data);
      if (document.getElementById("applyOnSave").checked) {
        await applyCurrent();
      } else {
        setStatus("Guardado");
      }
    }

    async function maybeSave() {
      if (state.dirty) {
        await saveCurrent();
      }
    }

    async function applyCurrent() {
      if (!state.run) return;
      const data = await fetchJson(`/api/runs/${encodeURIComponent(state.run)}/records/${state.index}/apply-db`, {
        method: "POST"
      });
      fillForm(data);
      setStatus("Registro impactado en base");
    }

    async function applyAllReviewed() {
      if (!state.run) return;
      await maybeSave();
      const data = await fetchJson(`/api/runs/${encodeURIComponent(state.run)}/apply-db-reviewed`, {
        method: "POST"
      });
      document.getElementById("summaryText").textContent =
        "Pendientes: " + data.summary.pending_count +
        " | Revisados: " + data.summary.reviewed_count +
        " | Impactados: " + data.summary.impacted_count;
      setStatus("Impactados " + data.applied_count + " registros");
      await loadRecord(state.run, state.index);
    }

    document.getElementById("refreshRuns").addEventListener("click", loadRuns);
    document.getElementById("zoomRange").addEventListener("input", updateZoomPanel);
    document.getElementById("runSelect").addEventListener("change", async (event) => {
      await maybeSave();
      await loadRecord(event.target.value, 0);
    });
    document.getElementById("saveBtn").addEventListener("click", saveCurrent);
    document.getElementById("applyCurrentBtn").addEventListener("click", async () => {
      await maybeSave();
      await applyCurrent();
    });
    document.getElementById("applyAll").addEventListener("click", applyAllReviewed);
    document.getElementById("prevBtn").addEventListener("click", async () => {
      await maybeSave();
      await loadRecord(state.run, Math.max(0, state.index - 1));
    });
    document.getElementById("nextBtn").addEventListener("click", async () => {
      await maybeSave();
      await loadRecord(state.run, Math.min(state.total - 1, state.index + 1));
    });
    document.querySelectorAll("#bp, #aclaracion, #documento, #fecha_entrega, #vinculo").forEach(element => {
      element.addEventListener("input", markDirty);
      element.addEventListener("change", () => {
        if (element.id === "bp" && element.checked) document.getElementById("vinculo").value = "";
        markDirty();
      });
    });
    loadRuns().catch(error => setStatus(error.message));
  </script>
</body>
</html>
""".replace("__VINCULOS__", ", ".join(json.dumps(item) for item in VALID_VINCULOS))


def build_app(output_dir: Path, failed_dir: Optional[Path] = None) -> FastAPI:
    app = FastAPI(title="Scan Indexer Review")

    @app.get("/", response_class=HTMLResponse)
    async def index() -> HTMLResponse:
        return HTMLResponse(HTML_PAGE)

    @app.get("/api/runs")
    async def api_runs() -> Dict[str, Any]:
        runs = list_run_dirs(output_dir)
        return {
            "runs": [run_summary(run_dir, load_run_records(run_dir)) for run_dir in runs],
            "latest": None if not runs else runs[0].name,
        }

    @app.get("/api/runs/{run_name}/records/{index}")
    async def api_record(run_name: str, index: int) -> Dict[str, Any]:
        run_dir = output_dir / run_name
        if not run_dir.exists():
            raise HTTPException(status_code=404, detail="No existe la carpeta seleccionada.")
        records = load_run_records(run_dir)
        return record_payload(run_dir, records, index, failed_dir)

    @app.get("/api/runs/{run_name}/image/{index}")
    async def api_image(run_name: str, index: int) -> Response:
        run_dir = output_dir / run_name
        if not run_dir.exists():
            raise HTTPException(status_code=404, detail="No existe la carpeta seleccionada.")
        records = load_run_records(run_dir)
        if index < 0 or index >= len(records):
            raise HTTPException(status_code=404, detail="Registro fuera de rango.")
        image_path = resolve_image_path(run_dir, records[index], failed_dir)
        if image_path is None or not image_path.exists():
            raise HTTPException(status_code=404, detail="No se encontro la imagen del registro.")
        media_type = mimetypes.guess_type(str(image_path))[0] or "image/jpeg"
        return Response(content=image_path.read_bytes(), media_type=media_type)

    @app.put("/api/runs/{run_name}/records/{index}")
    async def api_update_record(run_name: str, index: int, payload: RecordUpdate) -> Dict[str, Any]:
        run_dir = output_dir / run_name
        if not run_dir.exists():
            raise HTTPException(status_code=404, detail="No existe la carpeta seleccionada.")
        records = load_run_records(run_dir)
        if index < 0 or index >= len(records):
            raise HTTPException(status_code=404, detail="Registro fuera de rango.")
        apply_update_to_record(records[index], payload)
        save_run_records(run_dir, records)
        return record_payload(run_dir, records, index, failed_dir)

    @app.post("/api/runs/{run_name}/records/{index}/apply-db")
    async def api_apply_current(run_name: str, index: int) -> Dict[str, Any]:
        run_dir = output_dir / run_name
        records = load_run_records(run_dir)
        if index < 0 or index >= len(records):
            raise HTTPException(status_code=404, detail="Registro fuera de rango.")
        try:
            apply_record_to_database(records[index])
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        save_run_records(run_dir, records)
        return record_payload(run_dir, records, index, failed_dir)

    @app.post("/api/runs/{run_name}/apply-db-reviewed")
    async def api_apply_reviewed(run_name: str) -> Dict[str, Any]:
        run_dir = output_dir / run_name
        records = load_run_records(run_dir)
        applied_count = 0
        for record in records:
            if record.review_status != "reviewed" or record.db_applied:
                continue
            apply_record_to_database(record)
            applied_count += 1
        save_run_records(run_dir, records)
        return {
            "applied_count": applied_count,
            "summary": run_summary(run_dir, records),
        }

    return app


def main() -> None:
    load_dotenv()
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.failed_dir.mkdir(parents=True, exist_ok=True)
    if find_latest_results_dir(args.output_dir) is None:
        raise SystemExit(f"No se encontraron corridas con results.json dentro de {args.output_dir}")
    app = build_app(args.output_dir, args.failed_dir)
    uvicorn.run(app, host=args.host, port=args.port, reload=False)


if __name__ == "__main__":
    main()
