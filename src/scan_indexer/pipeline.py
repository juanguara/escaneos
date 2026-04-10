from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from datetime import date, datetime, timedelta
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import cv2
import numpy as np
from openai import OpenAI
from PIL import Image, ImageStat
import pypdfium2 as pdfium
import psycopg
from pydantic import BaseModel, ValidationError
from dotenv import load_dotenv
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

try:
    import zxingcpp
except ImportError:  # pragma: no cover - optional dependency
    zxingcpp = None


LOGGER = logging.getLogger("scan_indexer")
console = Console()
SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".webp"}
SUPPORTED_PDF_EXTENSIONS = {".pdf"}
PRIMARY_BARCODE_PATTERN = re.compile(r"PST\d{8}", re.IGNORECASE)
SECONDARY_BARCODE_PATTERN = re.compile(r"\d{6,20}")
SECONDARY_BARCODE_STRICT_PATTERN = re.compile(r"\d{9}")
VALID_VINCULOS = [
    "Carta poder",
    "Empleada/o",
    "Encargado/a",
    "Esposo/a",
    "Familiar",
    "Hermano/a",
    "Hija",
    "Hijo",
    "Hijo/a",
    "Inquilino/a",
    "Madre",
    "Otro",
    "Padre",
    "Portero/a",
    "Seguridad",
    "Titular",
    "Vecina",
    "Vecino",
]
VINCULO_ALIASES = {
    "cartapoder": "Carta poder",
    "empleada": "Empleada/o",
    "empleado": "Empleada/o",
    "encargada": "Encargado/a",
    "encargado": "Encargado/a",
    "esposa": "Esposo/a",
    "esposo": "Esposo/a",
    "familiar": "Familiar",
    "hermana": "Hermano/a",
    "hermano": "Hermano/a",
    "hija": "Hija",
    "hijo": "Hijo",
    "inquilina": "Inquilino/a",
    "inquilino": "Inquilino/a",
    "madre": "Madre",
    "otro": "Otro",
    "padre": "Padre",
    "portera": "Portero/a",
    "portero": "Portero/a",
    "seguridad": "Seguridad",
    "titular": "Titular",
    "vecina": "Vecina",
    "vecino": "Vecino",
}


class HandwritingFields(BaseModel):
    fecha_entrega: Optional[str] = None
    aclaracion: Optional[str] = None
    documento: Optional[str] = None
    vinculo: Optional[str] = None
    bp: bool = False
    referencias: Optional[str] = None
    firma_presente: Optional[bool] = None
    observaciones: Optional[str] = None


@dataclass
class DocumentResult:
    archivo: str
    pagina: Optional[int]
    estado: str
    motivo_error: Optional[str]
    ai_status: Optional[str]
    db_status: Optional[str]
    order_update_status: Optional[str]
    barcode_1_exists_db: Optional[bool]
    purchase_order_id: Optional[int]
    fecha_entrega: Optional[str]
    fecha_entrega_status: Optional[str]
    barcode_2_status: Optional[str]
    barcode_1: Optional[str]
    barcode_2: Optional[str]
    aclaracion: Optional[str]
    documento: Optional[str]
    vinculo: Optional[str]
    bp: Optional[str]
    referencias: Optional[str]
    firma_presente: Optional[bool]
    observaciones: Optional[str]
    model: Optional[str]
    review_status: Optional[str]
    reviewed_at: Optional[str]
    db_applied: Optional[bool]

    def to_record(self) -> Dict[str, Any]:
        return {
            "archivo": self.archivo,
            "pagina": self.pagina,
            "estado": self.estado,
            "motivo_error": self.motivo_error,
            "ai_status": self.ai_status,
            "db_status": self.db_status,
            "order_update_status": self.order_update_status,
            "barcode_1_exists_db": self.barcode_1_exists_db,
            "purchase_order_id": self.purchase_order_id,
            "fecha_entrega": self.fecha_entrega,
            "fecha_entrega_status": self.fecha_entrega_status,
            "barcode_2_status": self.barcode_2_status,
            "barcode_1": self.barcode_1,
            "barcode_2": self.barcode_2,
            "aclaracion": self.aclaracion,
            "documento": self.documento,
            "vinculo": self.vinculo,
            "bp": self.bp,
            "referencias": self.referencias,
            "firma_presente": self.firma_presente,
            "observaciones": self.observaciones,
            "model": self.model,
            "review_status": self.review_status,
            "reviewed_at": self.reviewed_at,
            "db_applied": self.db_applied,
        }

    @classmethod
    def from_record(cls, record: Dict[str, Any]) -> "DocumentResult":
        return cls(
            archivo=record.get("archivo"),
            pagina=record.get("pagina"),
            estado=record.get("estado"),
            motivo_error=record.get("motivo_error"),
            ai_status=record.get("ai_status"),
            db_status=record.get("db_status"),
            order_update_status=record.get("order_update_status"),
            barcode_1_exists_db=record.get("barcode_1_exists_db"),
            purchase_order_id=record.get("purchase_order_id"),
            fecha_entrega=record.get("fecha_entrega"),
            fecha_entrega_status=record.get("fecha_entrega_status"),
            barcode_2_status=record.get("barcode_2_status"),
            barcode_1=record.get("barcode_1"),
            barcode_2=record.get("barcode_2"),
            aclaracion=record.get("aclaracion"),
            documento=record.get("documento"),
            vinculo=record.get("vinculo"),
            bp=record.get("bp"),
            referencias=record.get("referencias"),
            firma_presente=record.get("firma_presente"),
            observaciones=record.get("observaciones"),
            model=record.get("model"),
            review_status=record.get("review_status"),
            reviewed_at=record.get("reviewed_at"),
            db_applied=record.get("db_applied"),
        )


@dataclass
class PlannedDbOperation:
    order_id: int
    archivo: str
    pagina: Optional[int]
    delivery_address_id: Optional[int]
    operations: List[Dict[str, Any]]

    def to_record(self) -> Dict[str, Any]:
        return {
            "order_id": self.order_id,
            "archivo": self.archivo,
            "pagina": self.pagina,
            "delivery_address_id": self.delivery_address_id,
            "operations": self.operations,
        }


@dataclass
class InputDocument:
    source_path: Path
    display_name: str
    page_number: Optional[int] = None
    rendered_image: Optional[Image.Image] = None

    @property
    def copy_name(self) -> str:
        if self.page_number is None:
            return self.source_path.name
        stem = self.source_path.stem
        return f"{stem}_p{self.page_number:03d}.jpg"


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Procesa documentos escaneados, extrae barcodes localmente y manuscrita con IA."
    )
    parser.add_argument("--input-dir", type=Path, help="Carpeta de entrada con imágenes o PDFs.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Carpeta destino para procesados.")
    parser.add_argument("--failed-dir", required=True, type=Path, help="Carpeta destino para fallados.")
    parser.add_argument(
        "--model",
        default="gpt-5.4-mini",
        help="Modelo OpenAI para reconocimiento manuscrito. Ej: gpt-5.4-mini, gpt-5.4-nano.",
    )
    parser.add_argument(
        "--api-base",
        default=None,
        help="URL base opcional compatible con OpenAI para usar otro backend.",
    )
    parser.add_argument(
        "--detail",
        choices=("low", "high", "auto"),
        default="high",
        help="Nivel de detalle de visión enviado al modelo.",
    )
    parser.add_argument(
        "--skip-ai",
        action="store_true",
        help="Procesa sólo la parte local. La extracción manuscrita queda vacía.",
    )
    parser.add_argument(
        "--copy-originals",
        action="store_true",
        help="Copia los archivos originales procesados a output-dir. El original nunca se mueve.",
    )
    parser.add_argument(
        "--skip-db-check",
        action="store_true",
        help="No valida barcode_1 contra PostgreSQL.",
    )
    parser.add_argument(
        "--apply-db-updates",
        action="store_true",
        help="Actualiza purchase_order_status y order_tracking para órdenes validadas en la base.",
    )
    parser.add_argument(
        "--dry-run-db-updates",
        action="store_true",
        help="No escribe en la base; genera un archivo con las transacciones SQL que ejecutaría.",
    )
    parser.add_argument(
        "--results-json-only",
        action="store_true",
        help="Reutiliza output-dir/results.json y omite el procesamiento de documentos.",
    )
    parser.add_argument("--verbose", action="store_true", help="Muestra logs más detallados.")
    return parser.parse_args()


def list_input_files(input_dir: Path) -> List[Path]:
    return sorted(
        [
            path
            for path in input_dir.iterdir()
            if path.is_file() and path.suffix.lower() in (SUPPORTED_EXTENSIONS | SUPPORTED_PDF_EXTENSIONS)
        ]
    )


def load_records_from_json(results_json_path: Path) -> List[DocumentResult]:
    payload = json.loads(results_json_path.read_text(encoding="utf-8"))
    return [DocumentResult.from_record(item) for item in payload]


def load_image(path: Path) -> Image.Image:
    image = Image.open(path)
    return image.convert("RGB")


def expand_inputs(input_files: Sequence[Path]) -> List[InputDocument]:
    expanded: List[InputDocument] = []
    for path in input_files:
        suffix = path.suffix.lower()
        if suffix in SUPPORTED_EXTENSIONS:
            expanded.append(InputDocument(source_path=path, display_name=path.name))
            continue
        if suffix in SUPPORTED_PDF_EXTENSIONS:
            expanded.extend(render_pdf_pages(path))
    return expanded


def render_pdf_pages(path: Path, scale: float = 3.0) -> List[InputDocument]:
    pdf = pdfium.PdfDocument(str(path))
    pages: List[InputDocument] = []
    try:
        for page_index in range(len(pdf)):
            page = pdf[page_index]
            try:
                bitmap = page.render(scale=scale)
                pil_image = bitmap.to_pil().convert("RGB")
                pil_image = trim_white_margins(pil_image)
            finally:
                page.close()
            pages.append(
                InputDocument(
                    source_path=path,
                    display_name=f"{path.name} [pag {page_index + 1}]",
                    page_number=page_index + 1,
                    rendered_image=pil_image,
                )
            )
    finally:
        pdf.close()
    return pages


def load_input_image(input_document: InputDocument) -> Image.Image:
    if input_document.rendered_image is not None:
        return input_document.rendered_image.copy()
    return load_image(input_document.source_path)


def trim_white_margins(image: Image.Image, white_threshold: int = 245, margin: int = 8) -> Image.Image:
    rgb = np.array(image.convert("RGB"))
    content_mask = np.any(rgb < white_threshold, axis=2)
    coords = np.argwhere(content_mask)
    if coords.size == 0:
        return image

    y1, x1 = coords.min(axis=0)
    y2, x2 = coords.max(axis=0)
    x1 = max(0, int(x1) - margin)
    y1 = max(0, int(y1) - margin)
    x2 = min(image.width, int(x2) + margin + 1)
    y2 = min(image.height, int(y2) + margin + 1)
    return image.crop((x1, y1, x2, y2))


def portrait_to_landscape(image: Image.Image) -> Image.Image:
    if image.width >= image.height:
        return image
    return image.rotate(90, expand=True)


def cyan_balance_score(image: Image.Image) -> float:
    width, height = image.size
    left = image.crop((0, 0, int(width * 0.3), int(height * 0.35)))
    right = image.crop((int(width * 0.7), 0, width, int(height * 0.35)))
    return cyan_score(left) - cyan_score(right)


def cyan_score(image: Image.Image) -> float:
    rgb = image.convert("RGB")
    total = 0
    score = 0.0
    for r, g, b in rgb.getdata():
        total += 1
        if g > 100 and b > 100 and (g - r) > 40 and (b - r) > 40:
            score += 1
    return score / max(total, 1)


def decode_barcodes(image: Image.Image) -> List[str]:
    detector = cv2.barcode_BarcodeDetector()
    values: List[str] = []
    width, height = image.size
    search_regions = [
        image,
        image.crop((int(width * 0.68), int(height * 0.05), width, int(height * 0.45))),
        image.crop((int(width * 0.63), int(height * 0.60), width, height)),
    ]

    for region in search_regions:
        for variant in barcode_variants(region):
            decode_with_opencv(detector, variant, values)
            decode_with_zxing(variant, values)

    return values


def decode_with_zxing(image_bgr: np.ndarray, values: List[str]) -> None:
    if zxingcpp is None:
        return

    try:
        image_rgb = cv2.cvtColor(image_bgr, cv2.COLOR_BGR2RGB)
        results = zxingcpp.read_barcodes(image_rgb)
        for result in results:
            text = getattr(result, "text", "")
            normalized = normalize_barcode_text(text)
            if normalized and normalized not in values:
                values.append(normalized)
    except Exception as exc:  # pragma: no cover - optional backend
        LOGGER.debug("ZXing no pudo resolver barcodes en una variante: %s", exc)


def find_expected_barcodes(image: Image.Image) -> Tuple[Optional[str], Optional[str], List[str]]:
    width, height = image.size
    top_right = image.crop((int(width * 0.62), 0, width, int(height * 0.40)))
    bottom_right = image.crop((int(width * 0.60), int(height * 0.58), width, height))

    top_reads = decode_barcodes(top_right)
    bottom_reads = decode_barcodes(bottom_right)
    full_reads = decode_barcodes(image)
    all_reads = dedupe_preserve_order(top_reads + bottom_reads + full_reads)

    primary = extract_primary_barcode(top_right, top_reads + full_reads)
    secondary = extract_secondary_barcode(bottom_right, bottom_reads + full_reads, exclude=primary)
    return primary, secondary, all_reads


def pick_primary_barcode(values: Sequence[str]) -> Optional[str]:
    candidates: List[str] = []
    for value in values:
        normalized = normalize_barcode_candidate(value)
        if PRIMARY_BARCODE_PATTERN.fullmatch(normalized):
            candidates.append(normalized.upper())
    if candidates:
        return candidates[0]

    relaxed_candidates: List[str] = []
    for value in values:
        normalized = normalize_barcode_candidate(value)
        if normalized.startswith("PST") and len(normalized) >= 6:
            relaxed_candidates.append(normalized.upper())
    return relaxed_candidates[0] if relaxed_candidates else None


def extract_primary_barcode(top_right: Image.Image, values: Sequence[str]) -> Optional[str]:
    primary = pick_primary_barcode(values)
    if primary:
        return primary

    for region in primary_barcode_text_regions(top_right):
        ocr_text = ocr_primary_barcode_text(region)
        primary = extract_pst_from_text(ocr_text)
        if primary:
            return primary

    ocr_text = ocr_image_text(top_right)
    return extract_pst_from_text(ocr_text)


def primary_barcode_text_regions(top_right: Image.Image) -> List[Image.Image]:
    width, height = top_right.size
    return [
        top_right,
        top_right.crop((int(width * 0.40), 0, width, int(height * 0.25))),
        top_right.crop((int(width * 0.42), int(height * 0.18), width, int(height * 0.42))),
    ]


def extract_secondary_barcode(bottom_right: Image.Image, values: Sequence[str], exclude: Optional[str]) -> Optional[str]:
    ocr_text = ocr_image_text(bottom_right)
    secondary_from_text = extract_numeric_code_from_text(ocr_text, exclude=exclude)
    if secondary_from_text:
        return secondary_from_text

    secondary = pick_secondary_barcode(values, exclude=exclude)
    if secondary:
        return secondary

    return None


def pick_secondary_barcode(values: Sequence[str], exclude: Optional[str]) -> Optional[str]:
    strict_candidates: List[str] = []
    for value in values:
        normalized = normalize_barcode_candidate(value)
        if exclude and normalized.upper() == exclude.upper():
            continue
        if SECONDARY_BARCODE_STRICT_PATTERN.fullmatch(normalized):
            strict_candidates.append(normalized)
    if strict_candidates:
        return strict_candidates[0]

    for value in values:
        normalized = normalize_barcode_candidate(value)
        if exclude and normalized.upper() == exclude.upper():
            continue
        if SECONDARY_BARCODE_PATTERN.fullmatch(normalized):
            return normalized
    return None


def normalize_barcode_candidate(value: Any) -> str:
    cleaned = str(value or "").strip().upper()
    cleaned = re.sub(r"[^A-Z0-9]", "", cleaned)
    if cleaned.startswith("PS") and len(cleaned) == 10 and cleaned[2:].isdigit():
        cleaned = "PST" + cleaned[2:]
    if cleaned.startswith("P57") and len(cleaned) == 11 and cleaned[3:].isdigit():
        cleaned = "PST" + cleaned[3:]
    if cleaned.startswith("PST") and len(cleaned) > 11:
        cleaned = cleaned[:11]
    return cleaned


def dedupe_preserve_order(values: Sequence[str]) -> List[str]:
    result: List[str] = []
    for value in values:
        if value not in result:
            result.append(value)
    return result


def ocr_image_text(image: Image.Image) -> str:
    tesseract_text = ocr_image_text_with_tesseract(image)
    if tesseract_text:
        return tesseract_text

    if os.name == "nt":
        LOGGER.debug("No se encontro Tesseract OCR en Windows.")
        return ""

    script_path = Path(__file__).resolve().parents[2] / "scripts" / "ocr_text.swift"
    if not script_path.exists():
        LOGGER.debug("No existe script OCR local en %s", script_path)
        return ""

    swift_path = shutil.which("swift") or "/usr/bin/swift"
    if not Path(swift_path).exists():
        LOGGER.debug("No se encontro ejecutable swift para OCR local.")
        return ""

    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as temp_file:
        temp_path = Path(temp_file.name)

    try:
        image.save(temp_path, format="JPEG", quality=95)
        completed = subprocess.run(
            [swift_path, str(script_path), str(temp_path)],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            LOGGER.debug("OCR local falló: %s", completed.stderr.strip())
            return ""
        return completed.stdout.strip()
    finally:
        temp_path.unlink(missing_ok=True)


def ocr_primary_barcode_text(image: Image.Image) -> str:
    return ocr_image_text_with_tesseract(
        image,
        psm="7",
        whitelist="PST0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    )


def ocr_image_text_with_tesseract(
    image: Image.Image,
    psm: str = "6",
    whitelist: Optional[str] = None,
) -> str:
    tesseract_path = shutil.which("tesseract")
    if not tesseract_path:
        return ""

    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as temp_file:
        temp_path = Path(temp_file.name)

    try:
        image.save(temp_path, format="PNG")
        command = [tesseract_path, str(temp_path), "stdout", "--psm", psm]
        if whitelist:
            command.extend(["-c", f"tessedit_char_whitelist={whitelist}"])
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=False,
        )
        if completed.returncode != 0:
            stderr_text = decode_subprocess_output(completed.stderr)
            LOGGER.debug("Tesseract OCR fallo: %s", stderr_text.strip())
            return ""
        return decode_subprocess_output(completed.stdout).strip()
    finally:
        temp_path.unlink(missing_ok=True)


def decode_subprocess_output(data: Optional[bytes]) -> str:
    if not data:
        return ""
    for encoding in ("utf-8", "cp1252", "latin-1"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="ignore")


def extract_pst_from_text(text: str) -> Optional[str]:
    normalized = normalize_ocr_text(text)
    normalized = normalized.replace("PS7", "PST")
    normalized = normalized.replace("P57", "PST")
    normalized = normalized.replace("PT", "PST")
    match = re.search(r"PST(\d{8})", normalized)
    if match:
        return f"PST{match.group(1)}"
    return None


def extract_numeric_code_from_text(text: str, exclude: Optional[str]) -> Optional[str]:
    normalized = normalize_ocr_text(text)
    matches = re.findall(r"\d{6,20}", normalized)
    ranked = sorted(matches, key=secondary_text_score, reverse=True)
    for candidate in ranked:
        if exclude and candidate.upper() == exclude.upper():
            continue
        return candidate
    return None


def secondary_text_score(candidate: str) -> Tuple[int, int]:
    exact_length = 2 if len(candidate) == 9 else 0
    likely_length = 1 if 8 <= len(candidate) <= 10 else 0
    return (exact_length, likely_length, -abs(len(candidate) - 9))


def normalize_ocr_text(text: str) -> str:
    normalized = text.upper()
    normalized = normalized.replace(" ", "")
    normalized = normalized.replace("\n", "")
    normalized = normalized.replace("O", "0")
    normalized = normalized.replace("I", "1")
    normalized = normalized.replace("L", "1")
    normalized = normalized.replace("Z", "2")
    return re.sub(r"[^A-Z0-9]", "", normalized)


def barcode_variants(image: Image.Image) -> List[np.ndarray]:
    rgb = np.array(image)
    gray = cv2.cvtColor(rgb, cv2.COLOR_RGB2GRAY)
    enlarged = cv2.resize(gray, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
    _, otsu = cv2.threshold(enlarged, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    adaptive = cv2.adaptiveThreshold(
        enlarged,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        31,
        11,
    )
    return [
        cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR),
        cv2.cvtColor(enlarged, cv2.COLOR_GRAY2BGR),
        cv2.cvtColor(otsu, cv2.COLOR_GRAY2BGR),
        cv2.cvtColor(adaptive, cv2.COLOR_GRAY2BGR),
    ]


def decode_with_opencv(detector: cv2.barcode_BarcodeDetector, image_bgr: np.ndarray, values: List[str]) -> None:
    try:
        multi_result = detector.detectAndDecodeMulti(image_bgr)
        ok = bool(multi_result[0]) if len(multi_result) >= 1 else False
        decoded_info = multi_result[1] if len(multi_result) >= 2 else []
        if ok and isinstance(decoded_info, (list, tuple)):
            for text in decoded_info:
                normalized = normalize_barcode_text(text)
                if normalized and normalized not in values:
                    values.append(normalized)
    except cv2.error:
        LOGGER.debug("OpenCV detectAndDecodeMulti no pudo resolver barcodes en una variante.")

    try:
        single_result = detector.detectAndDecode(image_bgr)
        if len(single_result) >= 2:
            decoded_info = single_result[0]
            points = single_result[1]
            ok = points is not None
        else:
            decoded_info = ""
            ok = False
        if ok:
            normalized = normalize_barcode_text(decoded_info)
            if normalized and normalized not in values:
                values.append(normalized)
    except cv2.error:
        LOGGER.debug("OpenCV detectAndDecode no pudo resolver barcodes en una variante.")


def normalize_barcode_text(value: Any) -> str:
    return str(value or "").strip()


def choose_best_orientation(image: Image.Image) -> Tuple[Image.Image, List[str], int]:
    candidates: List[Tuple[Image.Image, List[str], float, int]] = []
    for rotation in (0, 90, 180, 270):
        candidate = image.rotate(rotation, expand=True) if rotation else image.copy()
        candidate = portrait_to_landscape(candidate)
        barcodes = decode_barcodes(candidate)
        score = len(barcodes) * 10 + cyan_balance_score(candidate)
        candidates.append((candidate, barcodes, score, rotation))
    best = max(candidates, key=lambda item: item[2])
    return best[0], best[1], best[3]


def prepare_scanned_orientation(image: Image.Image) -> Tuple[Image.Image, List[str], int]:
    candidate = portrait_to_landscape(image.copy())
    barcodes = decode_barcodes(candidate)
    rotation = 90 if image.width < image.height else 0
    return candidate, barcodes, rotation


def expand_box(box: Tuple[int, int, int, int], width: int, height: int, margin: int = 12) -> Tuple[int, int, int, int]:
    x1, y1, x2, y2 = box
    return (
        max(0, x1 - margin),
        max(0, y1 - margin),
        min(width, x2 + margin),
        min(height, y2 + margin),
    )


def crop_regions(image: Image.Image, layout: str = "photo") -> Dict[str, Image.Image]:
    width, height = image.size
    if layout == "scan_pdf":
        regions = {
            "destinatario": (int(width * 0.04), int(height * 0.28), int(width * 0.52), int(height * 0.39)),
            "domicilio_bp": (int(width * 0.69), int(height * 0.28), int(width * 0.98), int(height * 0.40)),
            "fecha_entrega": (int(width * 0.38), int(height * 0.53), int(width * 0.68), int(height * 0.64)),
            "signature": (int(width * 0.05), int(height * 0.56), int(width * 0.46), int(height * 0.76)),
            "aclaracion": (int(width * 0.10), int(height * 0.74), int(width * 0.73), int(height * 0.86)),
            "documento": (int(width * 0.11), int(height * 0.86), int(width * 0.45), int(height * 0.98)),
            "vinculo": (int(width * 0.47), int(height * 0.56), int(width * 0.74), int(height * 0.74)),
            "observaciones": (int(width * 0.71), int(height * 0.52), int(width * 0.98), int(height * 0.96)),
        }
    else:
        regions = {
            "destinatario": (int(width * 0.05), int(height * 0.30), int(width * 0.44), int(height * 0.40)),
            "domicilio_bp": (int(width * 0.67), int(height * 0.30), int(width * 0.98), int(height * 0.42)),
            "fecha_entrega": (int(width * 0.46), int(height * 0.57), int(width * 0.73), int(height * 0.66)),
            "signature": (int(width * 0.09), int(height * 0.64), int(width * 0.45), int(height * 0.76)),
            "aclaracion": (int(width * 0.09), int(height * 0.75), int(width * 0.72), int(height * 0.89)),
            "documento": (int(width * 0.09), int(height * 0.87), int(width * 0.46), int(height * 0.98)),
            "vinculo": (int(width * 0.44), int(height * 0.64), int(width * 0.72), int(height * 0.78)),
            "observaciones": (int(width * 0.60), int(height * 0.58), int(width * 0.98), int(height * 0.92)),
        }
    return {
        name: image.crop(expand_box(box, width, height))
        for name, box in regions.items()
    }


def image_to_data_url(image: Image.Image) -> str:
    buffer = BytesIO()
    image.save(buffer, format="JPEG", quality=92)
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return f"data:image/jpeg;base64,{encoded}"


def has_signature_marks(signature_image: Image.Image) -> bool:
    width, height = signature_image.size
    focus = signature_image.crop(
        (
            int(width * 0.18),
            int(height * 0.05),
            int(width * 0.98),
            int(height * 0.78),
        )
    )
    grayscale = np.array(focus.convert("L"))
    stats = ImageStat.Stat(Image.fromarray(grayscale))
    dark_pixels = grayscale < 150
    dark_ratio = float(np.count_nonzero(dark_pixels)) / max(grayscale.size, 1)

    rgb = np.array(focus.convert("RGB"))
    color_delta = np.max(rgb, axis=2) - np.min(rgb, axis=2)
    colored_dark = np.logical_and(dark_pixels, color_delta > 18)
    colored_dark_ratio = float(np.count_nonzero(colored_dark)) / max(grayscale.size, 1)

    return (
        stats.stddev[0] > 22
        and dark_ratio > 0.008
        and colored_dark_ratio > 0.002
    )


def aclaracion_indicates_bp(aclaracion: Optional[str]) -> bool:
    if not aclaracion:
        return False

    if re.search(r"(?i)\bb\.?\s*p\.?\b", aclaracion):
        return True

    compact = re.sub(r"\s+", "", aclaracion.strip())
    if not compact:
        return False

    has_letters = bool(re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", compact))
    has_digits = bool(re.search(r"\d", compact))
    has_symbols = bool(re.search(r"[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9]", compact))

    if not has_letters and (has_digits or has_symbols):
        return True

    normalized = normalize_matching_text(aclaracion)
    has_digits = bool(re.search(r"\d", aclaracion))
    if any(token in normalized for token in ("bp", "bajopuerta", "ref")):
        return True
    if has_digits and any(token in normalized for token in ("esq", "vecin", "puerta")):
        return True

    return False


def build_prompt() -> str:
    vinculos = ", ".join(VALID_VINCULOS)
    return (
        "Extrae datos manuscritos de un acuse de entrega. "
        "Debes responder solo JSON válido. "
        "Campos requeridos: fecha_entrega, aclaracion, documento, vinculo, bp, referencias, firma_presente, observaciones. "
        "Reglas: "
        "0) La identificación es estrictamente por línea del formulario. No mezcles contenido entre líneas. "
        "0a) 'fecha_entrega' sale solo de la línea 'Fecha de Entrega'. "
        "1) 'documento' debe contener solo el número si es legible. "
        "1a) 'documento' sale solo de la línea 'Tipo y N° Doc'. Nunca tomes números escritos en la línea de 'Aclaración'. "
        f"2) 'vinculo' debe elegirse usando una de estas opciones: {vinculos}. "
        "2a) Si no coincide aproximadamente con ninguna opción, usa la que este mas aproximada. "
        "3) 'bp' debe ser true solo si no hay firma y el contenido indica entrega bajo puerta o referencias vecinales. "
        "4) Cuando no hay firma, las referencias suelen estar en la misma línea de aclaración pero más hacia la derecha. "
        "4a) A veces la referencia manuscrita de BP está en el recuadro de Domicilio, debajo del barcode principal. "
        "Si Recepción está vacío pero Domicilio tiene una nota manuscrita, úsala como aclaración/referencias y marca bp=true. "
        "5) 'firma_presente' true si la firma manuscrita es visible, false si no hay. "
        "6) Ignora el bloque de observaciones impreso; no aporta para los datos manuscritos. "
        "7) Los nombres de aclaración suelen ser nombres y apellidos argentinos en castellano; intenta aproximarlos si la letra es difícil. "
        "8) Si solo hay firma y no hay aclaración legible, usa null; luego el sistema podrá completar con el destinatario impreso. "
        "9) No inventes. Si un campo no es legible, usa null."
    )


def extract_handwriting_with_ai(
    client: OpenAI,
    model: str,
    detail: str,
    regions: Dict[str, Image.Image],
    signature_hint: bool,
    destinatario_hint: Optional[str],
) -> HandwritingFields:
    content: List[Dict[str, Any]] = [{"type": "input_text", "text": build_prompt()}]
    content.append({"type": "input_text", "text": f"Indicador local de firma visible: {signature_hint}."})
    if destinatario_hint:
        content.append({"type": "input_text", "text": f"Destinatario impreso detectado: {destinatario_hint}."})
    for name in ("fecha_entrega", "signature", "aclaracion", "documento", "vinculo", "domicilio_bp"):
        content.append({"type": "input_text", "text": f"Region: {name}"})
        content.append(
            {
                "type": "input_image",
                "image_url": image_to_data_url(regions[name]),
                "detail": detail,
            }
        )

    response = client.responses.create(
        model=model,
        input=[{"role": "user", "content": content}],
        temperature=0,
    )

    text = response.output_text.strip()
    text = strip_json_fence(text)
    payload = json.loads(text)
    return HandwritingFields.model_validate(payload)


def strip_json_fence(text: str) -> str:
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def clean_document_number(documento: Optional[str], aclaracion: Optional[str]) -> Optional[str]:
    if not documento:
        return None

    normalized = re.sub(r"\D", "", documento)
    if not normalized:
        return None

    if len(normalized) < 7:
        return None

    if aclaracion:
        aclaracion_digits = set(re.findall(r"\d+", aclaracion))
        if normalized in aclaracion_digits and len(normalized) <= 4:
            return None

    return normalized


def extract_document_number_from_text(text: str, aclaracion: Optional[str]) -> Optional[str]:
    if not text:
        return None

    normalized = normalize_ocr_text(text)
    matches = re.findall(r"\d{7,11}", normalized)
    if not matches:
        return None

    ranked = sorted(matches, key=lambda value: (len(value) == 8, len(value), value), reverse=True)
    for candidate in ranked:
        cleaned = clean_document_number(candidate, aclaracion)
        if cleaned:
            return cleaned
    return None


def normalize_vinculo(raw_value: Optional[str]) -> str:
    if not raw_value:
        return "Otro"

    normalized = normalize_matching_text(raw_value)
    if normalized in VINCULO_ALIASES:
        return VINCULO_ALIASES[normalized]

    for alias, option in VINCULO_ALIASES.items():
        if alias in normalized or normalized in alias:
            return option

    best_option = "Familiar"
    best_score = 0.0

    for option in VALID_VINCULOS:
        option_normalized = normalize_matching_text(option)
        score = similarity_score(normalized, option_normalized)
        if normalized == option_normalized:
            return option
        if normalized in option_normalized or option_normalized in normalized:
            score = max(score, 0.9)
        if score > best_score:
            best_score = score
            best_option = option

    if best_score >= 0.4:
        return best_option
    return "Otro"


def normalize_matching_text(text: str) -> str:
    normalized = text.lower().strip()
    replacements = str.maketrans(
        {
            "á": "a",
            "é": "e",
            "í": "i",
            "ó": "o",
            "ú": "u",
            "ü": "u",
            "/": "",
            " ": "",
            "-": "",
        }
    )
    return normalized.translate(replacements)


def similarity_score(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    common = sum(1 for a, b in zip(left, right) if a == b)
    prefix_score = common / max(len(left), len(right))
    overlap = len(set(left) & set(right)) / max(len(set(right)), 1)
    return max(prefix_score, overlap)


def clean_recipient_name(raw_value: Optional[str]) -> Optional[str]:
    if not raw_value:
        return None
    lines = [line.strip() for line in raw_value.splitlines() if line.strip()]
    if not lines:
        return None
    text = " ".join(lines)
    text = re.sub(r"(?i)\bdestinatario\b", "", text)
    text = re.sub(r"[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) < 4:
        return None
    normalized = normalize_matching_text(text)
    forbidden_tokens = {
        "recepcion",
        "cliente",
        "gcba",
        "dgai",
        "domicilio",
        "localidad",
        "codigopostal",
        "provincia",
        "fechadeentrega",
        "datosderecepcion",
        "firma",
        "aclaracion",
        "doc",
    }
    if any(token in normalized for token in forbidden_tokens):
        return None

    words = [word for word in text.split() if len(word) >= 2]
    if len(words) < 2:
        return None

    return text.title()


def clean_bp_reference_text(raw_value: Optional[str]) -> Optional[str]:
    if not raw_value:
        return None

    lines = [line.strip() for line in raw_value.splitlines() if line.strip()]
    if not lines:
        return None

    text = " ".join(lines)
    text = re.sub(r"(?i)\bdomicilio\b", "", text)
    text = re.sub(r"(?i)\bestados?\s+unidos\b", "", text)
    text = re.sub(r"(?i)\bprovincia\b", "", text)
    text = re.sub(r"(?i)\bbuenos\s+aires\b", "", text)
    text = re.sub(r"(?i)\bcodigo\s+postal\b", "", text)
    text = re.sub(r"(?i)\bdgai\b", "", text)
    text = re.sub(r"(?i)\bgai\b", "", text)
    text = re.sub(r"(?i)\b\d{3,5}\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" -_,.;:")
    if len(text) < 3:
        return None

    compact = re.sub(r"\s+", "", text)
    if len(compact) < 3:
        return None
    return text


def clean_aclaracion_reference_text(raw_value: Optional[str]) -> Optional[str]:
    if not raw_value:
        return None

    text = " ".join(line.strip() for line in raw_value.splitlines() if line.strip())
    return normalize_aclaracion_text(text)


def normalize_aclaracion_text(raw_value: Optional[str]) -> Optional[str]:
    if not raw_value:
        return None

    text = " ".join(line.strip() for line in str(raw_value).splitlines() if line.strip())
    text = re.sub(r"\b(?:ACLARACION|ACLARACIÓN|CLARACION|CLARACIÓN|ARACION|ARACIÓN|RACION|RACIÓN|ACION|ACIÓN|CION|CIÓN)\b", "", text)
    text = re.sub(r"(?i)\baclaraci[oó]n\b", "", text)
    text = re.sub(r"(?i)\bclaraci[oó]n\b", "", text)
    text = re.sub(r"(?i)\baraci[oó]n\b", "", text)
    text = re.sub(r"(?i)\braci[oó]n\b", "", text)
    text = re.sub(r"\bRACION\b", "", text)
    text = re.sub(r"(?i)\bracion\b", "", text)
    text = re.sub(r"\bACION\b", "", text)
    text = re.sub(r"\bCION\b", "", text)
    text = re.sub(r"(?i)\bb\.?\s*p\.?\b", " ", text)
    text = re.sub(r"(?i)\bes[qg9]\.?\b", "esq", text)
    text = re.sub(r"(?i)\be5q\b", "esq", text)
    text = normalize_consecutive_house_numbers(text)
    text = normalize_esq_tokens(text)
    text = re.sub(r"\s+", " ", text).strip(" -_,.;:")
    if len(text) < 2:
        return None
    return text


def normalize_consecutive_house_numbers(text: str) -> str:
    def replace_match(match: re.Match[str]) -> str:
        left = match.group(1)
        right = match.group(2)
        try:
            if int(right) == int(left) + 1:
                return f"{left}-{right}"
        except ValueError:
            return match.group(0)
        return match.group(0)

    return re.sub(r"\b(\d{1,5})\s*[-/ ]\s*(\d{1,5})\b", replace_match, text)


def normalize_esq_tokens(text: str) -> str:
    def replace_match(match: re.Match[str]) -> str:
        token = match.group(0)
        if any(char.isdigit() for char in token):
            return token
        return "Esq"

    return re.sub(r"\b[Ee][A-Za-zÁÉÍÓÚÜÑáéíóúüñ]{2}\b", replace_match, text)


def contains_reference_numbers(value: Optional[str]) -> bool:
    if not value:
        return False
    return bool(re.search(r"\d{2,5}", value))


def is_bp_marker_text(value: Optional[str]) -> bool:
    if not value:
        return False
    stripped = value.strip()
    if not stripped:
        return False
    if re.search(r"\d", stripped):
        return False
    compact = re.sub(r"[^A-Za-z]", "", stripped).lower()
    return compact == "bp"


def normalize_delivery_date(raw_value: Optional[str], today: Optional[date] = None) -> Tuple[str, str]:
    today = today or date.today()
    parsed = parse_delivery_date(raw_value)
    if parsed and is_valid_delivery_date(parsed, today):
        return parsed.strftime("%d/%m/%Y"), "ok"
    fallback = fallback_delivery_date(today)
    return fallback.strftime("%d/%m/%Y"), "adjusted"


def parse_delivery_date(raw_value: Optional[str]) -> Optional[date]:
    if not raw_value:
        return None

    text = raw_value.strip()
    parts = re.findall(r"\d+", text)
    if len(parts) >= 3:
        day_part, month_part, year_part = parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        day_part, month_part = parts[0], parts[1]
        year_part = str((date.today()).year)
    else:
        digits = re.sub(r"\D", "", text)
        if len(digits) in (2, 3, 4):
            if len(digits) == 2:
                day_part, month_part = digits[:1], digits[1:]
            elif len(digits) == 3:
                day_part, month_part = digits[:1], digits[1:]
            else:
                day_part, month_part = digits[:2], digits[2:]
            year_part = str((date.today()).year)
        elif len(digits) == 6:
            day_part, month_part, year_part = digits[:2], digits[2:4], digits[4:]
        elif len(digits) == 8:
            day_part, month_part, year_part = digits[:2], digits[2:4], digits[4:]
        else:
            return None

    try:
        day_value = int(day_part)
        month_value = int(month_part)
        year_value = int(year_part)
        if year_value < 100:
            year_value += 2000
        return date(year_value, month_value, day_value)
    except ValueError:
        return None


def resolve_delivery_date(
    ai_value: Optional[str],
    ocr_value: Optional[str],
    today: Optional[date] = None,
) -> Tuple[str, str]:
    today = today or date.today()
    for candidate in (ai_value, ocr_value):
        parsed = parse_delivery_date(candidate)
        if parsed and is_valid_delivery_date(parsed, today):
            return parsed.strftime("%d/%m/%Y"), "ok"
    fallback = fallback_delivery_date(today)
    return fallback.strftime("%d/%m/%Y"), "adjusted"


def is_valid_delivery_date(candidate: date, today: date) -> bool:
    if candidate > today:
        return False
    if candidate < today - timedelta(days=15):
        return False
    if candidate.weekday() == 6:
        return False
    return True


def fallback_delivery_date(today: date) -> date:
    candidate = today - timedelta(days=1)
    if candidate.weekday() == 6:
        candidate = today - timedelta(days=2)
    return candidate


def get_db_connection_string() -> Optional[str]:
    password = os.getenv("PPOSTAL_DB_PASSWORD") or os.getenv("PGPASSWORD")
    if not password:
        return None
    host = os.getenv("PPOSTAL_DB_HOST", "200.58.127.105")
    port = os.getenv("PPOSTAL_DB_PORT", "5432")
    dbname = os.getenv("PPOSTAL_DB_NAME", "ppostal")
    user = os.getenv("PPOSTAL_DB_USER", "postgres")
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


def fetch_purchase_orders(connection_string: str, barcodes: Sequence[str]) -> Dict[str, int]:
    unique_barcodes = sorted({barcode for barcode in barcodes if barcode})
    if not unique_barcodes:
        return {}

    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, barcode FROM purchase_order WHERE barcode = ANY(%s)",
                (unique_barcodes,),
            )
            return {str(row[1]): int(row[0]) for row in cur.fetchall()}


def mark_order_as_complied(conn: psycopg.Connection, order_id: int, result_json: Dict[str, Any]) -> None:
    attr5 = resolve_attr5(result_json)

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM purchase_order WHERE id = %s", (order_id,))
            purchase_order = cur.fetchone()
            if purchase_order is None:
                raise ValueError(f"No existe purchase_order.id={order_id}")

            cur.execute(
                """
                SELECT delivery_address_id
                FROM purchase_order_status
                WHERE purchase_order_id = %s
                  AND is_last = 1
                LIMIT 1
                """,
                (order_id,),
            )
            current_status = cur.fetchone()
            if current_status is None:
                raise ValueError(f"No existe purchase_order_status actual para purchase_order.id={order_id}")

            delivery_address_id = current_status[0]

            cur.execute(
                """
                UPDATE purchase_order_status
                SET is_last = 0
                WHERE purchase_order_id = %s
                """,
                (order_id,),
            )

            cur.execute(
                """
                INSERT INTO purchase_order_status (
                    purchase_order_status_id,
                    status_date,
                    purchase_order_id,
                    delivery_address_id,
                    purchase_order_status_cause_id,
                    username,
                    is_last
                ) VALUES (
                    9,
                    NOW(),
                    %s,
                    %s,
                    NULL,
                    'admin',
                    1
                )
                """,
                (order_id, delivery_address_id),
            )

            cur.execute(
                """
                UPDATE order_tracking
                SET is_last = 0
                WHERE order_id = %s
                  AND order_type_id = '1'
                """,
                (order_id,),
            )

            cur.execute(
                """
                INSERT INTO order_tracking (
                    order_type_id,
                    order_id,
                    status_time,
                    status_code,
                    status_type_id,
                    status_cause,
                    status_comments,
                    tracker_code,
                    attr1,
                    attr2,
                    attr3,
                    attr4,
                    attr5,
                    attr6,
                    attr7,
                    attr8,
                    attr9,
                    syncro_time,
                    syncro_id,
                    is_last,
                    username
                ) VALUES (
                    '1',
                    %s,
                    NOW(),
                    9,
                    1,
                    NULL,
                    NULL,
                    %s,
                    %s,
                    %s,
                    '00:00',
                    'aut',
                    %s,
                    NULL,
                    %s,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    1,
                    'admin'
                )
                """,
                (
                    order_id,
                    result_json.get("barcode_1"),
                    result_json.get("aclaracion"),
                    result_json.get("fecha_entrega"),
                    attr5,
                    result_json.get("vinculo"),
                ),
            )


def resolve_attr5(result_json: Dict[str, Any]) -> Optional[str]:
    bp_value = str(result_json.get("bp") or "").strip().upper()
    if bp_value == "SI":
        return "BP"
    documento = result_json.get("documento")
    return None if documento in ("", None) else str(documento)


def plan_order_as_complied(conn: psycopg.Connection, order_id: int, result_json: Dict[str, Any]) -> PlannedDbOperation:
    attr5 = resolve_attr5(result_json)

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM purchase_order WHERE id = %s", (order_id,))
        purchase_order = cur.fetchone()
        if purchase_order is None:
            raise ValueError(f"No existe purchase_order.id={order_id}")

        cur.execute(
            """
            SELECT delivery_address_id
            FROM purchase_order_status
            WHERE purchase_order_id = %s
              AND is_last = 1
            LIMIT 1
            """,
            (order_id,),
        )
        current_status = cur.fetchone()
        if current_status is None:
            raise ValueError(f"No existe purchase_order_status actual para purchase_order.id={order_id}")

    delivery_address_id = current_status[0]
    operations = [
        {
            "sql": "UPDATE purchase_order_status SET is_last = 0 WHERE purchase_order_id = %(order_id)s",
            "params": {"order_id": order_id},
        },
        {
            "sql": (
                "INSERT INTO purchase_order_status "
                "(purchase_order_status_id, status_date, purchase_order_id, delivery_address_id, "
                "purchase_order_status_cause_id, username, is_last) "
                "VALUES (9, NOW(), %(order_id)s, %(delivery_address_id)s, NULL, 'admin', 1)"
            ),
            "params": {"order_id": order_id, "delivery_address_id": delivery_address_id},
        },
        {
            "sql": "UPDATE order_tracking SET is_last = 0 WHERE order_id = %(order_id)s AND order_type_id = '1'",
            "params": {"order_id": order_id},
        },
        {
            "sql": (
                "INSERT INTO order_tracking "
                "(order_type_id, order_id, status_time, status_code, status_type_id, status_cause, "
                "status_comments, tracker_code, attr1, attr2, attr3, attr4, attr5, attr6, attr7, attr8, "
                "attr9, syncro_time, syncro_id, is_last, username) "
                "VALUES ('1', %(order_id)s, NOW(), 9, 1, NULL, NULL, %(tracker_code)s, %(attr1)s, %(attr2)s, "
                "'00:00', 'aut', %(attr5)s, NULL, %(attr7)s, NULL, NULL, NULL, NULL, 1, 'admin')"
            ),
            "params": {
                "order_id": order_id,
                "tracker_code": result_json.get("barcode_1"),
                "attr1": result_json.get("aclaracion"),
                "attr2": result_json.get("fecha_entrega"),
                "attr5": attr5,
                "attr7": result_json.get("vinculo"),
            },
        },
    ]

    return PlannedDbOperation(
        order_id=order_id,
        archivo=str(result_json.get("archivo")),
        pagina=result_json.get("pagina"),
        delivery_address_id=delivery_address_id,
        operations=operations,
    )


def validate_records_against_database(
    records: List[DocumentResult],
    document_lookup: Dict[Tuple[str, Optional[int]], InputDocument],
    failed_dir: Path,
    skip_db_check: bool,
) -> None:
    if skip_db_check:
        for record in records:
            if record.db_status is None:
                record.db_status = "skipped"
        return

    connection_string = get_db_connection_string()
    if not connection_string:
        LOGGER.warning(
            "No se configuró PPOSTAL_DB_PASSWORD/PGPASSWORD. Se omite validación contra PostgreSQL."
        )
        for record in records:
            if record.db_status is None:
                record.db_status = "missing_credentials"
        return

    ok_records = [record for record in records if record.estado == "ok" and record.barcode_1]
    try:
        purchase_orders = fetch_purchase_orders(connection_string, [record.barcode_1 for record in ok_records if record.barcode_1])
    except psycopg.Error as exc:
        LOGGER.error("Falló validación PostgreSQL: %s", exc)
        for record in records:
            if record.db_status is None:
                record.db_status = "db_error"
        return

    for record in records:
        if record.estado != "ok" or not record.barcode_1:
            if record.db_status is None:
                record.db_status = "not_checked"
            continue
        purchase_order_id = purchase_orders.get(record.barcode_1)
        exists = purchase_order_id is not None
        record.barcode_1_exists_db = exists
        record.purchase_order_id = purchase_order_id
        record.db_status = "found" if exists else "not_found"
        if not exists:
            record.estado = "fallado"
            record.motivo_error = "barcode_1 no existe en ppostal.purchase_order"
            key = (record.archivo, record.pagina)
            input_document = document_lookup.get(key)
            if input_document is not None:
                copy_if_requested(input_document, failed_dir)


def apply_database_updates(
    records: List[DocumentResult],
    connection_string: Optional[str],
    failed_dir: Path,
    document_lookup: Dict[Tuple[str, Optional[int]], InputDocument],
    apply_updates: bool,
    dry_run: bool,
) -> List[PlannedDbOperation]:
    planned_operations: List[PlannedDbOperation] = []
    if not apply_updates:
        for record in records:
            if record.order_update_status is None:
                record.order_update_status = "skipped"
        return planned_operations

    if not connection_string:
        LOGGER.warning("No se configuró conexión PostgreSQL. Se omiten updates de estado.")
        for record in records:
            if record.order_update_status is None:
                record.order_update_status = "missing_credentials"
        return planned_operations

    updatable_records = [
        record for record in records
        if record.estado == "ok" and record.purchase_order_id is not None
    ]

    try:
        with psycopg.connect(connection_string) as conn:
            for record in updatable_records:
                try:
                    if dry_run:
                        plan = plan_order_as_complied(conn, int(record.purchase_order_id), record.to_record())
                        planned_operations.append(plan)
                        record.order_update_status = "dry_run"
                        record.db_applied = False
                    else:
                        mark_order_as_complied(conn, int(record.purchase_order_id), record.to_record())
                        record.order_update_status = "updated"
                        record.db_applied = True
                except (psycopg.Error, ValueError) as exc:
                    LOGGER.error("Falló actualización DB para %s pag %s: %s", record.archivo, record.pagina, exc)
                    record.order_update_status = "update_error"
                    record.estado = "fallado"
                    record.motivo_error = f"falló actualización DB: {exc}"
                    key = (record.archivo, record.pagina)
                    input_document = document_lookup.get(key)
                    if input_document is not None:
                        copy_if_requested(input_document, failed_dir)
    except psycopg.Error as exc:
        LOGGER.error("No se pudo abrir conexión PostgreSQL para updates: %s", exc)
        for record in records:
            if record.order_update_status is None:
                record.order_update_status = "db_error"
    return planned_operations


def create_client(api_base: Optional[str]) -> OpenAI:
    kwargs: Dict[str, Any] = {}
    if api_base:
        kwargs["base_url"] = api_base
    return OpenAI(**kwargs)


def ensure_dirs(*dirs: Path) -> None:
    for directory in dirs:
        directory.mkdir(parents=True, exist_ok=True)


def create_run_output_dir(output_root_dir: Path, today: Optional[date] = None) -> Path:
    today = today or date.today()
    prefix = today.strftime("%Y-%m-%d")
    existing_numbers: List[int] = []
    for child in output_root_dir.iterdir():
        if not child.is_dir():
            continue
        match = re.fullmatch(rf"{re.escape(prefix)}_(\d+)", child.name)
        if match:
            existing_numbers.append(int(match.group(1)))
    next_number = (max(existing_numbers) + 1) if existing_numbers else 1
    run_dir = output_root_dir / f"{prefix}_{next_number:03d}"
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def find_latest_results_dir(output_root_dir: Path) -> Optional[Path]:
    direct_results = output_root_dir / "results.json"
    if direct_results.exists():
        return output_root_dir

    candidates = [
        child for child in output_root_dir.iterdir()
        if child.is_dir() and (child / "results.json").exists()
    ]
    if not candidates:
        return None
    return sorted(candidates, key=lambda path: path.name)[-1]


def output_extension_for_document(input_document: InputDocument) -> str:
    if input_document.page_number is not None:
        return ".jpg"
    return input_document.source_path.suffix.lower()


def build_non_conflicting_path(destination_dir: Path, filename: str) -> Path:
    candidate = destination_dir / filename
    if not candidate.exists():
        return candidate

    stem = candidate.stem
    suffix = candidate.suffix
    counter = 1
    while True:
        alternative = destination_dir / f"{stem}__retry{counter}{suffix}"
        if not alternative.exists():
            return alternative
        counter += 1


def copy_if_requested(input_document: InputDocument, destination_dir: Path, output_stem: Optional[str] = None) -> None:
    target_name = input_document.copy_name if not output_stem else f"{output_stem}{output_extension_for_document(input_document)}"
    if input_document.page_number is None:
        destination_path = destination_dir / target_name
        try:
            if input_document.source_path.resolve() == destination_path.resolve():
                destination_path = build_non_conflicting_path(destination_dir, target_name)
        except FileNotFoundError:
            pass
        if destination_path.exists():
            destination_path = build_non_conflicting_path(destination_dir, destination_path.name)
        shutil.copy2(input_document.source_path, destination_path)
        return
    if input_document.rendered_image is None:
        return
    output_path = build_non_conflicting_path(destination_dir, target_name)
    input_document.rendered_image.save(output_path, format="JPEG", quality=95)


def write_outputs(output_dir: Path, records: Sequence[DocumentResult], planned_operations: Optional[Sequence[PlannedDbOperation]] = None) -> Tuple[Path, Path]:
    json_path = output_dir / "results.json"
    excel_path = output_dir / "results.xlsx"
    dry_run_path = output_dir / "db_dry_run.json"

    json_payload = [record.to_record() for record in records]
    json_path.write_text(json.dumps(json_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    dataframe = pd.DataFrame(json_payload)
    dataframe.to_excel(excel_path, index=False)
    if planned_operations is not None:
        dry_run_payload = [item.to_record() for item in planned_operations]
        dry_run_path.write_text(json.dumps(dry_run_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return json_path, excel_path


def process_document(
    input_document: InputDocument,
    output_dir: Path,
    failed_dir: Path,
    client: Optional[OpenAI],
    model: str,
    detail: str,
    skip_ai: bool,
    copy_originals: bool,
    ai_enabled: bool,
) -> DocumentResult:
    LOGGER.info("Procesando %s", input_document.display_name)
    try:
        image = load_input_image(input_document)
        if input_document.page_number is not None and input_document.source_path.suffix.lower() in SUPPORTED_PDF_EXTENSIONS:
            oriented, _barcodes, rotation = prepare_scanned_orientation(image)
        else:
            oriented, _barcodes, rotation = choose_best_orientation(image)
        primary_barcode, secondary_barcode, barcodes = find_expected_barcodes(oriented)
        LOGGER.debug("Rotación elegida para %s: %s", input_document.display_name, rotation)
        LOGGER.debug("Barcodes detectados para %s: %s", input_document.display_name, barcodes)

        if not primary_barcode:
            raise ValueError(
                "No se pudo detectar el código principal con formato PST########."
            )
        barcode_2_status = "ok" if secondary_barcode and SECONDARY_BARCODE_STRICT_PATTERN.fullmatch(secondary_barcode) else "missing_or_unverified"

        layout = "scan_pdf" if input_document.page_number is not None and input_document.source_path.suffix.lower() in SUPPORTED_PDF_EXTENSIONS else "photo"
        regions = crop_regions(oriented, layout=layout)
        destinatario_impreso = clean_recipient_name(ocr_image_text(regions["destinatario"]))
        domicilio_bp_text = clean_bp_reference_text(ocr_image_text(regions["domicilio_bp"]))
        signature_hint = has_signature_marks(regions["signature"])
        fecha_ocr = ocr_image_text(regions["fecha_entrega"])
        aclaracion_ocr = clean_aclaracion_reference_text(ocr_image_text(regions["aclaracion"]))
        documento_ocr = ocr_image_text(regions["documento"])

        handwriting = HandwritingFields(firma_presente=signature_hint)
        ai_status = "disabled"
        if ai_enabled and not skip_ai:
            handwriting = extract_handwriting_with_ai(client, model, detail, regions, signature_hint, destinatario_impreso)
            if handwriting.firma_presente is None:
                handwriting.firma_presente = signature_hint
            else:
                handwriting.firma_presente = bool(handwriting.firma_presente or signature_hint)
            ai_status = "ok"
        elif not ai_enabled:
            ai_status = "missing_api_key"
        elif skip_ai:
            ai_status = "skipped"

        if handwriting.firma_presente is None:
            handwriting.firma_presente = signature_hint

        handwriting.aclaracion = normalize_aclaracion_text(handwriting.aclaracion)
        handwriting.referencias = normalize_aclaracion_text(handwriting.referencias)

        if aclaracion_indicates_bp(handwriting.aclaracion):
            handwriting.firma_presente = False
            handwriting.bp = True

        if handwriting.firma_presente is False:
            handwriting.bp = True
            if handwriting.aclaracion and not handwriting.referencias:
                handwriting.referencias = handwriting.aclaracion.strip()

        if is_bp_marker_text(handwriting.aclaracion):
            handwriting.aclaracion = None
        if is_bp_marker_text(handwriting.referencias):
            handwriting.referencias = None

        if aclaracion_ocr and contains_reference_numbers(aclaracion_ocr):
            current_has_numbers = contains_reference_numbers(handwriting.aclaracion)
            if handwriting.bp or handwriting.firma_presente is False or not current_has_numbers:
                handwriting.aclaracion = aclaracion_ocr
                if handwriting.bp and not handwriting.referencias:
                    handwriting.referencias = aclaracion_ocr

        if handwriting.bp and (not handwriting.aclaracion or not handwriting.aclaracion.strip()) and aclaracion_ocr:
            handwriting.aclaracion = aclaracion_ocr
            if not handwriting.referencias:
                handwriting.referencias = aclaracion_ocr

        if domicilio_bp_text and (not handwriting.aclaracion or not handwriting.aclaracion.strip()):
            handwriting.aclaracion = domicilio_bp_text
            handwriting.referencias = domicilio_bp_text
            handwriting.firma_presente = False
            handwriting.bp = True

        if handwriting.firma_presente and (not handwriting.aclaracion or not handwriting.aclaracion.strip()) and destinatario_impreso:
            handwriting.aclaracion = destinatario_impreso

        handwriting.aclaracion = normalize_aclaracion_text(handwriting.aclaracion)
        handwriting.referencias = normalize_aclaracion_text(handwriting.referencias)

        handwriting.documento = clean_document_number(handwriting.documento, handwriting.aclaracion)
        if handwriting.documento is None:
            handwriting.documento = extract_document_number_from_text(documento_ocr, handwriting.aclaracion)
        handwriting.vinculo = normalize_vinculo(handwriting.vinculo)
        fecha_entrega, fecha_entrega_status = resolve_delivery_date(handwriting.fecha_entrega, fecha_ocr)

        bp_value = "SI" if handwriting.bp else "NO"
        if bp_value == "SI":
            handwriting.vinculo = None

        if copy_originals:
            copy_if_requested(input_document, output_dir, output_stem=primary_barcode)

        return DocumentResult(
            archivo=input_document.source_path.name,
            pagina=input_document.page_number,
            estado="ok",
            motivo_error=None,
            ai_status=ai_status,
            db_status=None,
            order_update_status=None,
            barcode_1_exists_db=None,
            purchase_order_id=None,
            fecha_entrega=fecha_entrega,
            fecha_entrega_status=fecha_entrega_status,
            barcode_2_status=barcode_2_status,
            barcode_1=primary_barcode,
            barcode_2=secondary_barcode,
            aclaracion=handwriting.aclaracion,
            documento=handwriting.documento,
            vinculo=handwriting.vinculo,
            bp=bp_value,
            referencias=handwriting.referencias,
            firma_presente=handwriting.firma_presente,
            observaciones=handwriting.observaciones,
            model=None if skip_ai else model,
            review_status="pending",
            reviewed_at=None,
            db_applied=False,
        )
    except (ValidationError, json.JSONDecodeError, ValueError, RuntimeError) as exc:
        LOGGER.error("Falló %s: %s", input_document.display_name, exc)
        copy_if_requested(input_document, failed_dir)
        return DocumentResult(
            archivo=input_document.source_path.name,
            pagina=input_document.page_number,
            estado="fallado",
            motivo_error=str(exc),
            ai_status="error",
            db_status="not_checked",
            order_update_status="not_checked",
            barcode_1_exists_db=None,
            purchase_order_id=None,
            fecha_entrega=None,
            fecha_entrega_status="error",
            barcode_2_status="error",
            barcode_1=None,
            barcode_2=None,
            aclaracion=None,
            documento=None,
            vinculo=None,
            bp=None,
            referencias=None,
            firma_presente=None,
            observaciones=None,
            model=None if skip_ai else model,
            review_status="pending",
            reviewed_at=None,
            db_applied=False,
        )


def main() -> None:
    load_dotenv()
    args = parse_args()
    configure_logging(args.verbose)

    ensure_dirs(args.output_dir, args.failed_dir)
    effective_output_dir = args.output_dir
    documents: List[InputDocument] = []
    document_lookup: Dict[Tuple[str, Optional[int]], InputDocument] = {}

    client: Optional[OpenAI] = None
    ai_enabled = False
    if not args.skip_ai and not args.results_json_only:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            LOGGER.warning(
                "OPENAI_API_KEY no está definida. El proceso seguirá en modo local: se intentarán extraer barcodes y "
                "los campos manuscritos quedarán vacíos."
            )
        else:
            client = create_client(args.api_base)
            ai_enabled = True

    if args.results_json_only:
        latest_results_dir = find_latest_results_dir(args.output_dir)
        if latest_results_dir is None:
            raise SystemExit(
                f"No existe ningún results.json en {args.output_dir}. Ejecutá primero un procesamiento normal o quitá --results-json-only."
            )
        effective_output_dir = latest_results_dir
        results_json_path = effective_output_dir / "results.json"
        LOGGER.info("Reutilizando resultados existentes desde %s", results_json_path)
        records = load_records_from_json(results_json_path)
    else:
        effective_output_dir = create_run_output_dir(args.output_dir)
        LOGGER.info("La salida de esta corrida se guardará en %s", effective_output_dir)
        if args.input_dir is None:
            raise SystemExit("Debés indicar --input-dir, salvo que uses --results-json-only.")
        input_files = list_input_files(args.input_dir)
        if not input_files:
            raise SystemExit(f"No se encontraron imágenes o PDFs en {args.input_dir}")
        documents = expand_inputs(input_files)
        if not documents:
            raise SystemExit(f"No se pudieron expandir documentos desde {args.input_dir}")
        document_lookup = {(document.source_path.name, document.page_number): document for document in documents}
        records = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("Procesando documentos", total=len(documents))
            for input_document in documents:
                result = process_document(
                    input_document=input_document,
                    output_dir=effective_output_dir,
                    failed_dir=args.failed_dir,
                    client=client,
                    model=args.model,
                    detail=args.detail,
                    skip_ai=args.skip_ai,
                    copy_originals=args.copy_originals,
                    ai_enabled=ai_enabled,
                )
                records.append(result)
                progress.advance(task_id)

    connection_string = get_db_connection_string()
    validate_records_against_database(records, document_lookup, args.failed_dir, args.skip_db_check)
    planned_operations = apply_database_updates(
        records,
        connection_string,
        args.failed_dir,
        document_lookup,
        args.apply_db_updates or args.dry_run_db_updates,
        args.dry_run_db_updates,
    )
    json_path, excel_path = write_outputs(effective_output_dir, records, planned_operations)
    ok_count = len([record for record in records if record.estado == "ok"])
    failed_count = len(records) - ok_count
    console.print()
    console.print(f"[bold green]Procesados OK:[/bold green] {ok_count}")
    console.print(f"[bold red]Fallados:[/bold red] {failed_count}")
    console.print(f"[bold]Carpeta de corrida:[/bold] {effective_output_dir}")
    console.print(f"[bold]Excel:[/bold] {excel_path}")
    console.print(f"[bold]JSON:[/bold] {json_path}")


if __name__ == "__main__":
    main()
