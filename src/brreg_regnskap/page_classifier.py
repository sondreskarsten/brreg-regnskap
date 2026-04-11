"""Zero-cost page classification for Norwegian årsregnskap PDFs.

Four layers, all from pixel metadata and spatial statistics — no OCR, no ML, no API calls.

Layer 1 — Source split (100% accuracy, 243/243 pages, 20 entities):
    BRREG pages rendered at 1728×2312, company pages at 1653×{2140-2341}.
    Deterministic from embedded image dimensions via fitz.extract_image().

Layer 2 — BRREG type by positional order (96%, 112/117):
    p1=generell_info, p2=resultatregnskap, p3=balanse,
    p4=balanse if n_brreg≥5, p5=balanse if n_brreg≥10, rest=noter.

Layer 3 — Platform identification via footer perceptual hash (70% classified):
    average_hash of bottom 12% of second company page, matched by hamming
    distance ≤5 against known platform templates from 205-PDF calibration.

Layer 4 — Zone segmentation via projection profiles (88% has_table):
    Horizontal projection → ink/whitespace runs → vertical blocks.
    Column gap detection on interior region → table/title/text per block.

Usage:
    from brreg_regnskap.page_classifier import build_manifest, segment_page_zones

    manifest = build_manifest(pdf_bytes)
    # manifest["brreg_last_page"] → int
    # manifest["platform"] → str
    # manifest["pages"] → list of page dicts with source, type, zones

    # Or just get zones for one page:
    zones = segment_page_zones(gray_image)
    # [{"y0": 248, "y1": 292, "label": "title"}, {"y0": 660, "y1": 759, "label": "table", ...}]
"""

from __future__ import annotations

import io
from collections import defaultdict

import fitz
import numpy as np
from PIL import Image

BRREG_WIDTH = 1728

PLATFORM_HASHES = {
    "0000000000000000": "fiken_tripletex_conta",
    "fdfff8f8ffffffff": "orgnr_footer",
    "000001000000ffff": "pipe_orgnr",
    "ffff3c3cffffffff": "visma_finale_a",
    "ffff0000ffffffff": "visma_finale_b",
    "ffff1c9fffffffff": "visma_finale_c",
}

COMPANY_REVISJON_HEIGHTS = {2140, 2337}


def _img_dims(doc: fitz.Document, page_idx: int) -> tuple[int, int]:
    imgs = doc[page_idx].get_images()
    if not imgs:
        return 0, 0
    info = doc.extract_image(imgs[0][0])
    return info["width"], info["height"]


def _page_to_gray(doc: fitz.Document, page_idx: int) -> np.ndarray:
    imgs = doc[page_idx].get_images()
    info = doc.extract_image(imgs[0][0])
    pil = Image.open(io.BytesIO(info["image"])).convert("L")
    return np.array(pil, dtype=np.float64)


def _detect_platform(doc: fitz.Document, second_company_idx: int) -> str:
    try:
        import imagehash
    except ImportError:
        return "imagehash_not_installed"

    imgs = doc[second_company_idx].get_images()
    if not imgs:
        return "no_image"
    info = doc.extract_image(imgs[0][0])
    pil = Image.open(io.BytesIO(info["image"]))
    w, h = pil.size
    footer = pil.crop((0, int(h * 0.88), w, h))
    ah = imagehash.average_hash(footer, hash_size=8)

    for known_hex, label in PLATFORM_HASHES.items():
        known = imagehash.hex_to_hash(known_hex)
        if ah - known <= 5:
            return label
    return f"unknown_{ah}"


def segment_page_zones(
    gray: np.ndarray,
    min_gap: int = 40,
    min_block: int = 15,
    col_gap_min: int = 40,
) -> list[dict]:
    """Segment a grayscale page image into table/title/text zones.

    Args:
        gray: 2D numpy array (uint8 or float64), full-resolution page image.
        min_gap: Minimum whitespace rows to split blocks.
        min_block: Minimum block height in pixels.
        col_gap_min: Minimum column gap width for table detection.

    Returns:
        List of zone dicts: {"y0", "y1", "label", "n_col_gaps", ...}
    """
    h, w = gray.shape
    row_means = gray.mean(axis=1)
    has_ink = row_means < 245

    blocks = []
    in_block = False
    block_start = 0
    gap_count = 0

    for i in range(h):
        if has_ink[i]:
            if not in_block:
                block_start = i
                in_block = True
            gap_count = 0
        else:
            if in_block:
                gap_count += 1
                if gap_count > min_gap:
                    block_end = i - gap_count
                    if block_end - block_start >= min_block:
                        blocks.append((block_start, block_end))
                    in_block = False
                    gap_count = 0
    if in_block:
        block_end = h - 1
        if block_end - block_start >= min_block:
            blocks.append((block_start, block_end))

    zones = []
    for y0, y1 in blocks:
        block = gray[y0:y1, :]
        bh, bw = block.shape
        if bh < 10:
            continue

        margin_l = int(bw * 0.05)
        margin_r = int(bw * 0.95)
        interior = block[:, margin_l:margin_r]
        v_proj = interior.mean(axis=0)

        is_white = v_proj > 248
        col_gaps = []
        in_g = False
        g_start = 0
        for ci in range(len(is_white)):
            if is_white[ci]:
                if not in_g:
                    g_start = ci
                    in_g = True
            else:
                if in_g:
                    g_len = ci - g_start
                    g_pos = (g_start + ci) / 2 / len(is_white)
                    if g_len >= col_gap_min and 0.1 < g_pos < 0.9:
                        col_gaps.append(g_len)
                    in_g = False

        col_means = block.mean(axis=1)
        ink_rows = col_means < 240
        transitions = np.diff(ink_rows.astype(int))
        line_starts = np.where(transitions == 1)[0]

        if bh < 80 and len(line_starts) <= 3:
            label = "title"
        elif len(col_gaps) >= 2:
            label = "table"
        elif len(col_gaps) == 1 and col_gaps[0] > 100:
            label = "table"
        else:
            label = "text"

        zones.append({
            "y0": int(y0),
            "y1": int(y1),
            "label": label,
            "height": int(bh),
            "n_col_gaps": len(col_gaps),
            "n_lines": int(len(line_starts)),
        })

    return zones


def build_manifest(pdf_bytes: bytes) -> dict:
    """Build a complete page manifest from PDF bytes. Zero API calls.

    Returns dict with keys:
        brreg_last_page: int
        n_brreg: int
        n_company: int
        total_pages: int
        platform: str
        pages: list of page dicts
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")

    page_dims = []
    for i in range(doc.page_count):
        w, h = _img_dims(doc, i)
        page_dims.append({"page": i + 1, "w": w, "h": h})

    brreg_last = 0
    for pd in page_dims:
        if pd["w"] == BRREG_WIDTH:
            brreg_last = pd["page"]
        else:
            break
    n_brreg = brreg_last

    company_pages = [pd for pd in page_dims if pd["page"] > brreg_last]
    if len(company_pages) >= 2:
        platform = _detect_platform(doc, company_pages[1]["page"] - 1)
    elif len(company_pages) == 1:
        platform = "single_page_company"
    else:
        platform = "brreg_only"

    pages = []
    for pd in page_dims:
        if pd["page"] <= brreg_last:
            source = "brreg"
            pg = pd["page"]
            if pg == 1:
                ptype = "generell_info"
                has_table = False
            elif pg == 2:
                ptype = "resultatregnskap"
                has_table = True
            elif pg == 3:
                ptype = "balanse"
                has_table = True
            elif pg == 4:
                ptype = "balanse" if n_brreg >= 5 else "noter"
                has_table = True
            elif pg == 5 and n_brreg >= 10:
                ptype = "balanse"
                has_table = True
            else:
                ptype = "noter"
                has_table = True
        else:
            source = "company"
            if pd["h"] in COMPANY_REVISJON_HEIGHTS:
                ptype = "revisjonsberetning"
                has_table = False
            else:
                ptype = None
                has_table = None

        gray = _page_to_gray(doc, pd["page"] - 1)
        zones = segment_page_zones(gray)

        if has_table is None:
            has_table = any(z["label"] == "table" for z in zones)
        if ptype == "generell_info":
            has_table = False

        pages.append({
            "page": pd["page"],
            "source": source,
            "type": ptype,
            "has_table": has_table,
            "img_w": pd["w"],
            "img_h": pd["h"],
            "zones": zones,
        })

    doc.close()
    return {
        "brreg_last_page": brreg_last,
        "n_brreg": n_brreg,
        "n_company": len(page_dims) - brreg_last,
        "total_pages": len(page_dims),
        "platform": platform,
        "pages": pages,
    }
