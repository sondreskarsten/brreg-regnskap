"""DETR layout detection experiment for Norwegian årsregnskap pages.

Detects 11 element types per page (Table, Text, Title, Section-header,
Page-footer, etc.) with bounding boxes. Produces a spatial manifest that
enables column assignment verification without Gemini or Document AI.

SETUP (run first):
    pip install transformers torch timm Pillow --break-system-packages

USAGE:
    GOOGLE_APPLICATION_CREDENTIALS=creds.json python experiments/run_detr_layout.py

Model: cmarkea/detr-layout-detection (~170MB, runs on CPU)
Speed: ~2-3s per page on CPU
Cost: $0 (local inference)

Elements detected:
    Caption, Footnote, Formula, List-item, Page-footer, Page-header,
    Picture, Section-header, Table, Text, Title

Output: experiments/results/detr_layout_results.json
        experiments/results/detr_manifests.json (page-level classification)

The manifest output can be compared against the Gemini classification
results in docs/manifest_classification_experiment.md to validate
whether a free local model matches Gemini's page-type detection.
"""

from __future__ import annotations

import io
import json
import time
from pathlib import Path

import fitz
from PIL import Image

ENTITIES = {
    "988054631": "Bonord",
    "981472470": "ECITLAW",
    "968613189": "Alliance",
    "987591102": "Silvercoin",
    "988775460": "Rødskifer",
}

RESULTS_DIR = Path(__file__).parent / "results"


def load_model():
    from transformers import AutoImageProcessor, AutoModelForObjectDetection
    import torch

    print("Loading cmarkea/detr-layout-detection...")
    t0 = time.time()
    processor = AutoImageProcessor.from_pretrained("cmarkea/detr-layout-detection")
    model = AutoModelForObjectDetection.from_pretrained("cmarkea/detr-layout-detection")
    model.eval()
    print(f"Loaded in {time.time() - t0:.1f}s")
    return processor, model


def classify_page_from_elements(elements: dict[str, int]) -> str:
    has_table = elements.get("Table", 0) > 0
    text_blocks = elements.get("Text", 0)
    has_title = elements.get("Title", 0) > 0
    has_section_header = elements.get("Section-header", 0) > 0

    if has_table and text_blocks <= 2:
        return "financial_table"
    if has_table and text_blocks > 2:
        return "mixed_table_text"
    if text_blocks > 3 and not has_table:
        return "narrative"
    if has_title and text_blocks <= 2:
        return "cover_or_header"
    return "other"


def detect_page(processor, model, pil_image: Image.Image, threshold: float = 0.5):
    import torch

    inputs = processor(images=pil_image, return_tensors="pt")
    with torch.no_grad():
        outputs = model(**inputs)

    target_sizes = torch.tensor([pil_image.size[::-1]])
    results = processor.post_process_object_detection(
        outputs, target_sizes=target_sizes, threshold=threshold
    )[0]

    labels = model.config.id2label
    elements: dict[str, int] = {}
    detections = []

    for score, label, box in zip(results["scores"], results["labels"], results["boxes"]):
        lbl = labels[label.item()]
        elements[lbl] = elements.get(lbl, 0) + 1
        detections.append({
            "label": lbl,
            "confidence": round(score.item(), 3),
            "bbox": [round(x.item(), 1) for x in box],
        })

    return elements, detections


def run_on_entity(processor, model, pdf_bytes: bytes, orgnr: str, name: str):
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages = []

    for i in range(doc.page_count):
        imgs = doc[i].get_images()
        if not imgs:
            pages.append({
                "page": i + 1,
                "elements": {},
                "page_type": "blank",
                "detections": [],
            })
            continue

        img_data = doc.extract_image(imgs[0][0])
        pil = Image.open(io.BytesIO(img_data["image"])).convert("RGB")

        t0 = time.time()
        elements, detections = detect_page(processor, model, pil)
        elapsed = time.time() - t0

        page_type = classify_page_from_elements(elements)
        pages.append({
            "page": i + 1,
            "elements": elements,
            "page_type": page_type,
            "detections": detections,
            "elapsed_s": round(elapsed, 2),
        })

    doc.close()
    return {"orgnr": orgnr, "name": name, "n_pages": len(pages), "pages": pages}


def main():
    from google.cloud import storage

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    gcs = storage.Client()
    bucket = gcs.bucket("brreg-regnskap")
    processor, model = load_model()

    all_results = {}
    all_manifests = {}

    for orgnr, name in ENTITIES.items():
        print(f"\n{'=' * 50}")
        print(f"{name} ({orgnr})")

        pdf = bucket.blob(f"regnskap/{orgnr}/aarsregnskap_2024.pdf").download_as_bytes()
        result = run_on_entity(processor, model, pdf, orgnr, name)
        all_results[orgnr] = result

        manifest = []
        for p in result["pages"]:
            el_str = ", ".join(f"{k}={v}" for k, v in sorted(p["elements"].items()))
            print(f"  p{p['page']:>2}: {p['page_type']:>20}  [{el_str}]  {p.get('elapsed_s', 0):.1f}s")
            manifest.append({
                "page": p["page"],
                "page_type": p["page_type"],
                "has_table": p["elements"].get("Table", 0) > 0,
                "n_tables": p["elements"].get("Table", 0),
                "n_text_blocks": p["elements"].get("Text", 0),
                "n_titles": p["elements"].get("Title", 0),
            })
        all_manifests[orgnr] = {"name": name, "pages": manifest}

    with open(RESULTS_DIR / "detr_layout_results.json", "w") as f:
        json.dump(all_results, f, indent=2, default=str)

    with open(RESULTS_DIR / "detr_manifests.json", "w") as f:
        json.dump(all_manifests, f, indent=2)

    # Compare with Gemini manifests
    print("\n" + "=" * 60)
    print("DETR vs Gemini page classification comparison")
    print("=" * 60)
    try:
        gemini_manifests = json.loads(
            bucket.blob("notes/benchmark/page_manifests.json").download_as_text()
        )
        for orgnr in ENTITIES:
            gm = gemini_manifests.get(orgnr, {})
            dm = all_manifests.get(orgnr, {})
            if gm.get("pages") == "NOT_RUN":
                continue
            name = ENTITIES[orgnr]
            print(f"\n{name}:")
            print(f"  {'page':>4} {'Gemini':>25} {'DETR':>25} {'table_match':>12}")
            for gp, dp in zip(gm.get("pages", []), dm.get("pages", [])):
                g_type = f"{gp['source']}/{gp['type']}"
                d_type = dp["page_type"]
                g_tbl = gp.get("has_table", False)
                d_tbl = dp.get("has_table", False)
                match = "✓" if g_tbl == d_tbl else "✗"
                print(f"  p{gp['page']:>2}: {g_type:>25} {d_type:>25} {match:>12}")
    except Exception as e:
        print(f"Could not load Gemini manifests: {e}")

    print(f"\nResults saved to {RESULTS_DIR}/")


if __name__ == "__main__":
    main()
