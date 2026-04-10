"""Storage abstraction layer wrapping fsspec.

Provides a unified interface for reading/writing files to local filesystem,
S3, or GCS. The backend is determined automatically from the path prefix.

Implementation notes:
    - Uses fsspec.core.url_to_fs() to parse path prefixes into filesystem objects.
    - For S3: requires s3fs (pip install brreg-regnskap[s3])
    - For GCS: requires gcsfs (pip install brreg-regnskap[gcs])
    - Local paths work with the default fsspec filesystem.
    - check_credentials() should attempt a lightweight operation (e.g. ls on the prefix)
      and raise a clear error with setup instructions if it fails.
    - All write operations should be atomic where the backend supports it.
      S3 and GCS provide atomic single-object PUT with strong read-after-write consistency.
    - For local filesystem, write to a temp file then rename for atomicity.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from datetime import UTC, datetime

import fsspec  # type: ignore[import-untyped]

from brreg_regnskap.config import Settings, StorageBackendType


class CredentialError(Exception):
    """Raised when storage credentials are missing or invalid."""


class StorageBackend:
    """Unified file storage interface backed by fsspec.

    Usage:
        storage = StorageBackend.from_settings(settings)
        storage.check_credentials()
        storage.write_bytes("s3://bucket/path/file.json", b'{"key": "value"}')
        data = storage.read_bytes("s3://bucket/path/file.json")
    """

    def __init__(self, fs: fsspec.AbstractFileSystem, root_path: str) -> None:
        self._fs = fs
        self._root_path = root_path
        self._protocol = self._fs.protocol
        if isinstance(self._protocol, tuple):
            self._protocol = self._protocol[0]

    @classmethod
    def from_settings(cls, settings: Settings) -> StorageBackend:
        """Create a StorageBackend from application settings.

        Uses google.cloud.storage SDK for GCS (avoids gcsfs/aiohttp DNS issues
        in proxy environments). Falls back to fsspec for S3 and local.
        """
        if settings.backend_type == StorageBackendType.GCS:
            return GCSNativeBackend(root_path=settings.storage_path)
        fs, _ = fsspec.core.url_to_fs(settings.storage_path)
        return cls(fs=fs, root_path=settings.storage_path)

    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        return self._fs

    def _to_fs_path(self, path: str) -> str:
        """Strip protocol prefix for fsspec operations.

        fsspec methods expect paths without the protocol prefix.
        e.g. "s3://bucket/key" → "bucket/key", "./data/file" → "./data/file"
        """
        for prefix in ("s3://", "gs://", "gcs://"):
            if path.startswith(prefix):
                return path[len(prefix) :]
        return path

    def check_credentials(self) -> None:
        """Verify that credentials are configured and the storage path is accessible.

        Raises CredentialError with setup instructions if access fails.
        For local filesystem, creates the root directory if it doesn't exist.
        """
        fs_path = self._to_fs_path(self._root_path)
        if self._protocol in ("file", ""):
            self._fs.mkdirs(fs_path, exist_ok=True)
            return

        backend = StorageBackendType.S3 if self._protocol == "s3" else StorageBackendType.GCS
        try:
            self._fs.ls(fs_path)
        except FileNotFoundError:
            pass
        except Exception as exc:
            raise CredentialError(
                f"{self._credential_help_message(backend)}\n\nOriginal error: {exc}"
            ) from exc

    def write_bytes(self, path: str, data: bytes) -> None:
        """Write raw bytes to the given path atomically.

        For local filesystem: write to temp file, then rename.
        For S3/GCS: single PUT operation (inherently atomic).
        """
        fs_path = self._to_fs_path(path)

        if self._protocol in ("file", ""):
            parent = os.path.dirname(fs_path)
            if parent:
                self._fs.mkdirs(parent, exist_ok=True)
            tmp_path = fs_path + f".tmp.{uuid.uuid4().hex[:8]}"
            with self._fs.open(tmp_path, "wb") as f:
                f.write(data)
            self._fs.mv(tmp_path, fs_path)
            return

        parent = fs_path.rsplit("/", 1)[0] if "/" in fs_path else ""
        if parent and self._protocol in ("file", ""):
            self._fs.mkdirs(parent, exist_ok=True)
        with self._fs.open(fs_path, "wb") as f:
            f.write(data)

    def read_bytes(self, path: str) -> bytes:
        """Read raw bytes from the given path.

        Raises FileNotFoundError if the path does not exist.
        """
        fs_path = self._to_fs_path(path)
        if not self._fs.exists(fs_path):
            raise FileNotFoundError(f"No such file: {path}")
        with self._fs.open(fs_path, "rb") as f:
            return bytes(f.read())

    def exists(self, path: str) -> bool:
        """Check if a file exists at the given path."""
        fs_path = self._to_fs_path(path)
        return bool(self._fs.exists(fs_path))

    def list_dir(self, prefix: str) -> list[str]:
        """List all files under the given prefix.

        Returns full paths including protocol prefix.
        """
        fs_path = self._to_fs_path(prefix)
        try:
            entries = self._fs.glob(fs_path.rstrip("/") + "/**")
        except FileNotFoundError:
            return []
        if not entries:
            try:
                entries = self._fs.ls(fs_path, detail=False)
            except FileNotFoundError:
                return []

        proto_prefix = ""
        if self._protocol and self._protocol not in ("file", ""):
            proto_prefix = f"{self._protocol}://"

        return [f"{proto_prefix}{e}" for e in entries]

    def delete(self, path: str) -> None:
        """Delete the file at the given path. No-op if it doesn't exist."""
        fs_path = self._to_fs_path(path)
        with contextlib.suppress(FileNotFoundError):
            self._fs.rm(fs_path)

    def modified_time(self, path: str) -> datetime | None:
        """Return the last modified time of the file as a UTC datetime.

        Returns None if the file doesn't exist or mtime is unavailable.
        """
        fs_path = self._to_fs_path(path)
        try:
            info = self._fs.info(fs_path)
        except FileNotFoundError:
            return None
        mtime = info.get("mtime") or info.get("updated") or info.get("LastModified")
        if mtime is None:
            return None
        if isinstance(mtime, (int, float)):
            return datetime.fromtimestamp(mtime, tz=UTC)
        if isinstance(mtime, datetime):
            if mtime.tzinfo is None:
                return mtime.replace(tzinfo=UTC)
            return mtime
        if isinstance(mtime, str):
            return datetime.fromisoformat(mtime.replace("Z", "+00:00"))
        return None

    def rename(self, src: str, dst: str) -> None:
        """Rename/move a file. Used for archiving corrections.

        For S3/GCS this is a copy+delete (no native rename).
        """
        src_path = self._to_fs_path(src)
        dst_path = self._to_fs_path(dst)

        dst_parent = dst_path.rsplit("/", 1)[0] if "/" in dst_path else ""
        if dst_parent and self._protocol in ("file", ""):
            self._fs.mkdirs(dst_parent, exist_ok=True)

        if self._protocol in ("file", ""):
            self._fs.mv(src_path, dst_path)
        else:
            self._fs.copy(src_path, dst_path)
            self._fs.rm(src_path)

    def _credential_help_message(self, backend: StorageBackendType) -> str:
        """Return human-readable setup instructions for the given backend."""
        if backend == StorageBackendType.S3:
            return (
                "S3 credentials not found. Configure one of:\n"
                "  - AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY env vars\n"
                "  - AWS IAM role (for EC2/ECS/Lambda)\n"
                "  - aws configure (AWS CLI)\n"
                "  - OIDC role-to-assume in GitHub Actions"
            )
        if backend == StorageBackendType.GCS:
            return (
                "GCS credentials not found. Configure one of:\n"
                "  - GOOGLE_APPLICATION_CREDENTIALS env var (path to service account JSON)\n"
                "  - gcloud auth application-default login\n"
                "  - Workload identity in GitHub Actions"
            )
        return "Local filesystem — no credentials needed."


class GCSNativeBackend(StorageBackend):
    """GCS backend using google.cloud.storage SDK directly.

    Avoids gcsfs/aiohttp which fail in proxy environments due to
    aiohttp DNS resolution bypassing HTTP_PROXY.
    """

    def __init__(self, root_path: str) -> None:
        self._root_path = root_path
        self._protocol = "gcs"
        stripped = root_path.replace("gs://", "")
        parts = stripped.split("/", 1)
        self._bucket_name = parts[0]
        self._prefix = parts[1] if len(parts) > 1 else ""
        self._fs = None
        self._client = None

    def _get_client(self):
        if self._client is None:
            from google.cloud import storage as gcs_storage
            creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if creds_path:
                from google.oauth2 import service_account
                creds = service_account.Credentials.from_service_account_file(creds_path)
                self._client = gcs_storage.Client(credentials=creds, project=creds.project_id)
            else:
                self._client = gcs_storage.Client()
        return self._client

    def _bucket(self):
        return self._get_client().bucket(self._bucket_name)

    def _to_blob_name(self, path: str) -> str:
        for prefix in ("gs://", "gcs://"):
            if path.startswith(prefix):
                path = path[len(prefix):]
        if path.startswith(self._bucket_name + "/"):
            path = path[len(self._bucket_name) + 1:]
        return path

    def check_credentials(self) -> None:
        try:
            self._bucket().exists()
        except Exception as exc:
            raise CredentialError(
                f"{self._credential_help_message(StorageBackendType.GCS)}\n\nOriginal error: {exc}"
            ) from exc

    def write_bytes(self, path: str, data: bytes) -> None:
        blob_name = self._to_blob_name(path)
        self._bucket().blob(blob_name).upload_from_string(data)

    def read_bytes(self, path: str) -> bytes:
        blob_name = self._to_blob_name(path)
        blob = self._bucket().blob(blob_name)
        if not blob.exists():
            raise FileNotFoundError(f"No such file: {path}")
        return blob.download_as_bytes()

    def exists(self, path: str) -> bool:
        blob_name = self._to_blob_name(path)
        return self._bucket().blob(blob_name).exists()

    def list_dir(self, prefix: str) -> list[str]:
        blob_prefix = self._to_blob_name(prefix)
        blobs = self._get_client().list_blobs(self._bucket_name, prefix=blob_prefix)
        return [f"gs://{self._bucket_name}/{b.name}" for b in blobs]

    def delete(self, path: str) -> None:
        blob_name = self._to_blob_name(path)
        blob = self._bucket().blob(blob_name)
        with contextlib.suppress(Exception):
            blob.delete()

    def modified_time(self, path: str) -> datetime | None:
        blob_name = self._to_blob_name(path)
        blob = self._bucket().blob(blob_name)
        blob.reload()
        return blob.updated

    def rename(self, src: str, dst: str) -> None:
        src_name = self._to_blob_name(src)
        dst_name = self._to_blob_name(dst)
        bucket = self._bucket()
        src_blob = bucket.blob(src_name)
        bucket.copy_blob(src_blob, bucket, dst_name)
        src_blob.delete()
