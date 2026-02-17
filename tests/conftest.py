"""Shared test fixtures for brreg-regnskap tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture
def enhet_json() -> dict:
    return json.loads((FIXTURES_DIR / "enhet_964118191.json").read_text())


@pytest.fixture
def regnskap_json() -> list:
    return json.loads((FIXTURES_DIR / "regnskap_964118191.json").read_text())


@pytest.fixture
def years_json() -> list:
    return json.loads((FIXTURES_DIR / "years_964118191.json").read_text())


@pytest.fixture
def tmp_storage_path(tmp_path: Path) -> str:
    return str(tmp_path / "test-storage")
