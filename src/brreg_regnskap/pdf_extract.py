"""Extract journalnr and year from PDF cover page via OCR.

BRREG årsregnskap PDFs have a standard cover page from Brønnøysundregistrene
with the journal number and accounting year. The pages are image-based (no
embedded text), so OCR is required.

Dependencies:
    pip install PyMuPDF pytesseract Pillow
    apt-get install tesseract-ocr  (system package)
"""

from __future__ import annotations

import io
import re

import structlog

logger = structlog.get_logger()


def extract_journal_info(pdf_data: bytes) -> dict[str, str]:
    """Extract year and journalnr from the first page of a BRREG PDF.

    Args:
        pdf_data: Raw PDF bytes.

    Returns:
        {"year": "2024", "journalnr": "2025414236"} or "NA" for missing fields.
        journalnr is normalized: spaces removed.
    """
    import fitz
    import pytesseract
    from PIL import Image

    doc = fitz.open(stream=pdf_data, filetype="pdf")
    page = doc[0]
    pix = page.get_pixmap(dpi=300)
    img = Image.open(io.BytesIO(pix.tobytes("png")))
    doc.close()

    text = pytesseract.image_to_string(img, lang="eng")

    year_match = re.search(r"REGNSKAPS.RET\s+(\d{4})", text)
    journal_match = re.search(r"Journalnummer\s*:\s*([\d\s]+)", text)

    year = year_match.group(1) if year_match else "NA"
    journalnr = journal_match.group(1).strip().replace(" ", "") if journal_match else "NA"

    return {"year": year, "journalnr": journalnr}
