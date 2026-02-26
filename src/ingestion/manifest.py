"""Manifest-based incremental ingestion tracking."""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path

import duckdb

logger = logging.getLogger("ingest_transactions")


@dataclass
class ManifestEntry:
    """Represents a single file's manifest entry."""

    file_path: str
    file_size_bytes: int
    file_hash: str
    file_mtime: float
    run_id: str
    processed_at: str
    status: str
    error_message: str | None = None


def compute_file_hash(path: Path, chunk_size: int = 65536) -> str:
    """Compute SHA-256 hash of a file by streaming raw bytes.

    Args:
        path: Path to the file to hash.
        chunk_size: Size of chunks to read (default 64KB).

    Returns:
        Lowercase hex digest of the file contents.
    """
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


class ManifestStore:
    """Manages file manifest state in DuckDB."""

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Initialize with a DuckDB connection.

        Args:
            conn: An open DuckDB connection (shared with pipeline).
        """
        self._conn = conn

    def create_tables(self) -> None:
        """Create file_manifest and manifest_metadata tables if they don't exist."""
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS file_manifest (
                file_path        VARCHAR PRIMARY KEY,
                file_size_bytes  BIGINT NOT NULL,
                file_hash        VARCHAR NOT NULL,
                file_mtime       DOUBLE NOT NULL,
                run_id           VARCHAR NOT NULL,
                processed_at     TIMESTAMPTZ NOT NULL,
                status VARCHAR NOT NULL CHECK (status IN ('pending', 'success', 'failed')),
                error_message    VARCHAR
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS manifest_metadata (
                source_dir       VARCHAR PRIMARY KEY,
                watermark_mtime  DOUBLE NOT NULL,
                updated_at       TIMESTAMPTZ NOT NULL
            )
            """
        )
        logger.info("Manifest tables created/verified")

    def upsert_pending(
        self,
        file_path: str,
        file_hash: str,
        file_size_bytes: int,
        file_mtime: float,
        run_id: str,
    ) -> None:
        """Insert or update a manifest entry with status='pending'.

        This is phase 1 of the two-phase commit — called before file processing.

        Args:
            file_path: Relative path of the file.
            file_hash: SHA-256 hash of the file.
            file_size_bytes: Size of the file in bytes.
            file_mtime: Unix timestamp of file modification time.
            run_id: Current pipeline run ID.
        """
        self._conn.execute(
            """
            INSERT INTO file_manifest (
                file_path, file_size_bytes, file_hash, file_mtime,
                run_id, processed_at, status, error_message
            )
            VALUES (?, ?, ?, ?, ?, NOW(), 'pending', NULL)
            ON CONFLICT (file_path) DO UPDATE SET
                file_size_bytes = excluded.file_size_bytes,
                file_hash = excluded.file_hash,
                file_mtime = excluded.file_mtime,
                run_id = excluded.run_id,
                processed_at = excluded.processed_at,
                status = 'pending',
                error_message = NULL
            """,
            [file_path, file_size_bytes, file_hash, file_mtime, run_id],
        )

    def mark_success(self, file_path: str) -> None:
        """Mark a file as successfully processed (phase 2 - success path).

        Args:
            file_path: Relative path of the file.
        """
        self._conn.execute(
            """
            UPDATE file_manifest
            SET status = 'success', processed_at = NOW()
            WHERE file_path = ?
            """,
            [file_path],
        )

    def mark_failed(self, file_path: str, error_message: str) -> None:
        """Mark a file as failed (phase 2 - failure path).

        Args:
            file_path: Relative path of the file.
            error_message: Description of the error.
        """
        self._conn.execute(
            """
            UPDATE file_manifest
            SET status = 'failed', error_message = ?, processed_at = NOW()
            WHERE file_path = ?
            """,
            [error_message, file_path],
        )

    def get_manifest_entry(self, file_path: str) -> ManifestEntry | None:
        """Retrieve a manifest entry by file path.

        Args:
            file_path: Relative path of the file.

        Returns:
            ManifestEntry if found, None otherwise.
        """
        row = self._conn.execute(
            """
            SELECT file_path, file_size_bytes, file_hash, file_mtime,
                   run_id, processed_at, status, error_message
            FROM file_manifest
            WHERE file_path = ?
            """,
            [file_path],
        ).fetchone()

        if row is None:
            return None

        return ManifestEntry(
            file_path=row[0],
            file_size_bytes=row[1],
            file_hash=row[2],
            file_mtime=row[3],
            run_id=row[4],
            processed_at=str(row[5]),
            status=row[6],
            error_message=row[7],
        )

    def should_skip(self, file_path: str, current_hash: str) -> bool:
        """Determine if a file should be skipped based on manifest state.

        Args:
            file_path: Relative path of the file.
            current_hash: Current SHA-256 hash of the file.

        Returns:
            True if file should be skipped (already success with same hash).
        """
        entry = self.get_manifest_entry(file_path)
        if entry is None:
            return False
        if entry.status != "success":
            return False
        return entry.file_hash == current_hash

    def reset_all_to_pending(self, run_id: str) -> None:
        """Reset all manifest entries to 'pending' status for full refresh.

        Does NOT delete rows — uses UPDATE per Clarification Q3.

        Args:
            run_id: New run ID to assign to all entries.
        """
        self._conn.execute(
            """
            UPDATE file_manifest
            SET status = 'pending', run_id = ?, error_message = NULL, processed_at = NOW()
            """,
            [run_id],
        )
        logger.info("Reset all manifest entries to pending for run %s", run_id)

    def get_watermark(self, source_dir: str) -> float | None:
        """Get the watermark mtime for a source directory.

        Args:
            source_dir: Path to the source directory.

        Returns:
            Watermark mtime if exists, None otherwise.
        """
        row = self._conn.execute(
            """
            SELECT watermark_mtime FROM manifest_metadata WHERE source_dir = ?
            """,
            [source_dir],
        ).fetchone()

        return row[0] if row else None

    def update_watermark(self, source_dir: str, mtime: float) -> None:
        """Update the watermark for a source directory.

        Args:
            source_dir: Path to the source directory.
            mtime: Maximum file mtime to set as watermark.
        """
        self._conn.execute(
            """
            INSERT INTO manifest_metadata (source_dir, watermark_mtime, updated_at)
            VALUES (?, ?, NOW())
            ON CONFLICT (source_dir) DO UPDATE SET
                watermark_mtime = excluded.watermark_mtime,
                updated_at = excluded.updated_at
            """,
            [source_dir, mtime],
        )
        logger.info("Updated watermark for %s to %s", source_dir, mtime)

    def delete_watermark(self, source_dir: str) -> None:
        """Delete the watermark row for a source directory.

        Called during full-refresh to force re-evaluation of all files.

        Args:
            source_dir: Path to the source directory.
        """
        self._conn.execute(
            """
            DELETE FROM manifest_metadata WHERE source_dir = ?
            """,
            [source_dir],
        )
        logger.info("Deleted watermark for %s (full-refresh)", source_dir)
