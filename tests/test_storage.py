"""Tests for storage abstraction layer."""

from __future__ import annotations

from pathlib import Path

import pytest

from brreg_regnskap.config import Settings
from brreg_regnskap.storage import StorageBackend


class TestLocalStorage:
    def _make_storage(self, tmp_path: Path) -> StorageBackend:
        s = Settings(storage_path=str(tmp_path / "store"))
        return StorageBackend.from_settings(s)

    def test_check_credentials_creates_dir(self, tmp_path: Path) -> None:
        storage = self._make_storage(tmp_path)
        storage.check_credentials()
        assert (tmp_path / "store").is_dir()

    def test_write_and_read_bytes(self, tmp_path: Path) -> None:
        storage = self._make_storage(tmp_path)
        storage.check_credentials()
        path = str(tmp_path / "store" / "test.txt")
        storage.write_bytes(path, b"hello")
        assert storage.read_bytes(path) == b"hello"

    def test_read_missing_raises(self, tmp_path: Path) -> None:
        storage = self._make_storage(tmp_path)
        storage.check_credentials()
        with pytest.raises(FileNotFoundError):
            storage.read_bytes(str(tmp_path / "store" / "nope.txt"))

    def test_exists(self, tmp_path: Path) -> None:
        storage = self._make_storage(tmp_path)
        storage.check_credentials()
        path = str(tmp_path / "store" / "exists.txt")
        assert not storage.exists(path)
        storage.write_bytes(path, b"data")
        assert storage.exists(path)

    def test_delete(self, tmp_path: Path) -> None:
        storage = self._make_storage(tmp_path)
        storage.check_credentials()
        path = str(tmp_path / "store" / "del.txt")
        storage.write_bytes(path, b"data")
        storage.delete(path)
        assert not storage.exists(path)

    def test_delete_missing_is_noop(self, tmp_path: Path) -> None:
        storage = self._make_storage(tmp_path)
        storage.check_credentials()
        storage.delete(str(tmp_path / "store" / "nope.txt"))

    def test_rename(self, tmp_path: Path) -> None:
        storage = self._make_storage(tmp_path)
        storage.check_credentials()
        src = str(tmp_path / "store" / "a.txt")
        dst = str(tmp_path / "store" / "b.txt")
        storage.write_bytes(src, b"moved")
        storage.rename(src, dst)
        assert not storage.exists(src)
        assert storage.read_bytes(dst) == b"moved"

    def test_write_creates_subdirs(self, tmp_path: Path) -> None:
        storage = self._make_storage(tmp_path)
        storage.check_credentials()
        path = str(tmp_path / "store" / "deep" / "nested" / "file.txt")
        storage.write_bytes(path, b"nested")
        assert storage.read_bytes(path) == b"nested"

    def test_list_dir(self, tmp_path: Path) -> None:
        storage = self._make_storage(tmp_path)
        storage.check_credentials()
        storage.write_bytes(str(tmp_path / "store" / "a.txt"), b"a")
        storage.write_bytes(str(tmp_path / "store" / "b.txt"), b"b")
        result = storage.list_dir(str(tmp_path / "store"))
        assert len(result) >= 2
