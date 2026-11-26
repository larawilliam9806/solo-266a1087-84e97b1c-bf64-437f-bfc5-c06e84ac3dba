"""Batch PNG thumbnail generation service (single machine).

Scheduling model
----------------
Every uploaded PNG becomes a *job*.  A job is executed by *attempts*:

* ``first`` -- the initial execution.
* ``retry`` -- one concurrent make-up execution ("补跑") started while the
  first attempt is *still running* and has exceeded ``RETRY_AFTER_SECONDS``.

A global pool of ``SLOT_COUNT`` execution slots bounds how many attempts may
run at the same time.  A slot is released only when an attempt actually
finishes.  In particular, when a retry produces the thumbnail first, its slot
is *not* released by that success, and the slow first attempt keeps holding
its own slot until it returns.  This is intentional: "即使结果已可下载，仍在
运行的副本也占用槽位".

Dispatch priority, checked every tick and whenever a slot frees:

1. Jobs that have never been executed (``pending``) -- oldest first.
2. A single make-up attempt for a job whose first attempt is still running
   past the threshold -- oldest first, at most one retry per job.

Delivery rules:

* The first attempt to succeed wins.  The result is downloadable immediately
  and is labelled with its origin (``first`` / ``retry``).
* A job is marked ``failed`` only when every attempt it is entitled to has
  failed; a job that was retried therefore needs *both* attempts to fail.
* Retried jobs never get a third attempt.

Disk staging model
------------------
Uploaded originals are never accumulated in memory.  The upload handler reads
each multipart file in fixed-size chunks (``STAGING_CHUNK_BYTES``) and streams
them into a private staging directory on local disk:

    <staging_dir>/.incoming/<session>/<index>.part   # during upload
    <staging_dir>/<batch_id>/<job_id>.png            # once the batch commits

A file is validated (non-empty, size limit, PNG) straight from its staged
path, one file at a time.  The whole batch is registered and dispatched only
after *every* file has been written and validated; any bad image, oversized
file or disk-write failure rejects the entire batch and removes the session
directory.  Waiting jobs keep only a reference (the staged file path), never
the original bytes, so both the first attempt and a later retry open the same
on-disk original long after the upload request has ended.

The staged original is deleted when the job's last execution copy finishes
(success or failure); an empty batch directory is pruned at the same time.
The first successful thumbnail stays downloadable regardless of when the
original is cleaned up.

State is in-memory and process-local: run exactly one instance (the shipped
docker compose file does).  Staging leftovers from a crashed process are
swept at startup because the in-memory state that referenced them is gone.
"""

from __future__ import annotations

import asyncio
import io
import os
import secrets
import shutil
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import BinaryIO, Callable, Dict, List, Optional, Tuple

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse, Response
from PIL import Image, UnidentifiedImageError


# Indirection over the builtin open: tests swap it to simulate disk failures
# while everything else keeps using the real builtin (PIL, stdlib, ...).
_open = open


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Uploaded multipart files are drained in chunks of this many bytes so that a
# whole batch of large originals is never held in memory at once.
STAGING_CHUNK_BYTES = 1024 * 1024


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc
    if value < minimum:
        raise RuntimeError(f"{name} must be >= {minimum}, got {value}")
    return value


def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a number, got {raw!r}") from exc
    if value < minimum:
        raise RuntimeError(f"{name} must be >= {minimum}, got {value}")
    return value


@dataclass(frozen=True)
class Settings:
    # Total attempts allowed to execute at once, globally.
    slot_count: int = field(default_factory=lambda: max(1, _env_int("THUMBNAIL_SLOTS", 4)))
    # How long a first attempt must be running before one make-up attempt may
    # be dispatched concurrently (seconds).
    retry_after_seconds: float = field(
        default_factory=lambda: max(0.0, _env_float("THUMBNAIL_RETRY_AFTER_SECONDS", 10.0))
    )
    # Longest edge of the produced thumbnail, in pixels.
    max_edge: int = field(default_factory=lambda: max(1, _env_int("THUMBNAIL_MAX_EDGE", 256)))
    # Max bytes per uploaded file; 0 disables the check.
    max_upload_bytes: int = field(default_factory=lambda: _env_int("THUMBNAIL_MAX_UPLOAD_BYTES", 0))
    # Local-disk directory where uploaded originals are staged until the last
    # execution copy of a job finishes.
    staging_dir: str = field(
        default_factory=lambda: os.environ.get("THUMBNAIL_STAGING_DIR", ".staging")
    )


# ---------------------------------------------------------------------------
# Domain model
# ---------------------------------------------------------------------------

class JobState(str, Enum):
    PENDING = "pending"      # queued, never executed
    RUNNING = "running"      # first attempt in flight, no result yet
    RETRYING = "retrying"    # first attempt still running, make-up attempt in flight
    SUCCEEDED = "succeeded"  # at least one attempt succeeded
    FAILED = "failed"        # every entitled attempt failed


class AttemptKind(str, Enum):
    FIRST = "first"
    RETRY = "retry"


class AttemptState(str, Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass
class Attempt:
    kind: AttemptKind
    started_at: float
    state: AttemptState = AttemptState.RUNNING
    finished_at: Optional[float] = None
    error: Optional[str] = None

    def as_dict(self) -> dict:
        return {
            "kind": self.kind.value,
            "state": self.state.value,
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at) if self.finished_at is not None else None,
            "error": self.error,
        }


@dataclass
class Job:
    job_id: str
    filename: str
    # Reference to the staged original on local disk.  Cleared (and the file
    # removed) when the job's last execution copy finishes.
    source_path: Optional[str]
    state: JobState = JobState.PENDING
    attempts: List[Attempt] = field(default_factory=list)
    result: Optional[bytes] = None
    result_attempt: Optional[AttemptKind] = None
    result_size: Optional[Tuple[int, int]] = None
    error: Optional[str] = None

    @property
    def retry_dispatched(self) -> bool:
        return any(a.kind is AttemptKind.RETRY for a in self.attempts)

    @property
    def has_running_attempt(self) -> bool:
        return any(a.state is AttemptState.RUNNING for a in self.attempts)


@dataclass
class Batch:
    batch_id: str
    created_at: float
    job_ids: List[str] = field(default_factory=list)


@dataclass
class _StagedFile:
    filename: str
    partial_path: str
    handle: Optional[BinaryIO]
    size: int
    finalized: bool = False


class JobRejected(Exception):
    """Upload rejected before a batch was created (bad/oversized file or disk failure)."""


def _iso(ts: float) -> str:
    whole = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts))
    return f"{whole}.{int((ts % 1) * 1000):03d}Z"


# ---------------------------------------------------------------------------
# Staging area
# ---------------------------------------------------------------------------

class StagingStore:
    """On-disk staging area for uploaded originals.

    Layout::

        <root>/.incoming/<session>/<index>.part   # being uploaded / validated
        <root>/<batch_id>/<job_id>.png            # committed originals

    Everything lives on one filesystem so publishing a batch is a pair of
    atomic directory renames.
    """

    INCOMING_DIRNAME = ".incoming"

    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        self.incoming_root = os.path.join(self.root, self.INCOMING_DIRNAME)
        os.makedirs(self.incoming_root, exist_ok=True)
        # Scheduler state is process-local and does not survive a restart, so
        # anything left on disk by a previous process is orphaned garbage.
        self.sweep()

    def sweep(self) -> None:
        """Remove stale staging data (crashed uploads and orphaned batches)."""
        for base in (self.incoming_root, self.root):
            try:
                names = os.listdir(base)
            except FileNotFoundError:
                continue
            for name in names:
                if base is self.root and name == self.INCOMING_DIRNAME:
                    continue
                shutil.rmtree(os.path.join(base, name), ignore_errors=True)

    def new_session(self) -> Tuple[str, str]:
        session_id = secrets.token_hex(8)
        directory = os.path.join(self.incoming_root, session_id)
        os.makedirs(directory, exist_ok=False)
        return session_id, directory

    def partial_path(self, session_dir: str, index: int) -> str:
        return os.path.join(session_dir, f"{index}.part")

    def commit(self, session_dir: str, batch_id: str, job_ids: List[str]) -> Dict[str, str]:
        """Publish a fully validated session as a batch directory.

        Partials are renamed to ``<job_id>.png`` and the session directory is
        atomically moved to ``<root>/<batch_id>``.  Returns the final staged
        path keyed by job id.
        """
        for index, job_id in enumerate(job_ids):
            os.rename(
                self.partial_path(session_dir, index),
                os.path.join(session_dir, f"{job_id}.png"),
            )
        batch_dir = os.path.join(self.root, batch_id)
        os.rename(session_dir, batch_dir)
        return {job_id: os.path.join(batch_dir, f"{job_id}.png") for job_id in job_ids}

    def discard_session(self, session_dir: str) -> None:
        """Delete every partial file produced by a rejected upload."""
        shutil.rmtree(session_dir, ignore_errors=True)

    def release(self, path: str) -> None:
        """Remove one staged original; prune the batch directory once empty."""
        try:
            os.unlink(path)
        except FileNotFoundError:
            return
        batch_dir = os.path.dirname(path)
        try:
            # Only succeeds once every original of the batch is gone.
            os.rmdir(batch_dir)
        except OSError:
            pass

    def discard_all(self) -> None:
        self.sweep()


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render_thumbnail(source_path: str, max_edge: int) -> Tuple[bytes, Tuple[int, int]]:
    """Downscale the PNG at ``source_path`` to fit ``max_edge``.

    Returns ``(png_bytes, (w, h))``.  Aspect ratio is preserved; smaller
    images are not upscaled.  The original is read straight from disk, so the
    worker never needs the full payload handed to it in memory.
    """
    with Image.open(source_path) as img:
        img.load()
        if img.mode not in ("RGB", "RGBA", "L", "LA", "P"):
            img = img.convert("RGBA")
        width, height = img.size
        scale = min(1.0, max_edge / max(width, height))
        new_size = (max(1, round(width * scale)), max(1, round(height * scale)))
        if new_size != (width, height):
            img = img.resize(new_size, Image.Resampling.LANCZOS)
        out = io.BytesIO()
        img.save(out, format="PNG", optimize=True)
    return out.getvalue(), new_size


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class ThumbnailService:
    TICK_SECONDS = 0.05

    def __init__(
        self,
        settings: Settings,
        renderer: Optional[Callable[[str, int], Tuple[bytes, Tuple[int, int]]]] = None,
        staging: Optional[StagingStore] = None,
    ):
        self.settings = settings
        self._render = renderer or render_thumbnail
        self.staging = staging or StagingStore(settings.staging_dir)
        self._lock = threading.RLock()
        self._batches: Dict[str, Batch] = {}
        self._jobs: Dict[str, Job] = {}
        # Upload session id -> {"dir": str, "files": {index: _StagedFile}}
        self._uploads: Dict[str, dict] = {}
        self._in_flight = 0
        self._executor = ThreadPoolExecutor(
            max_workers=settings.slot_count, thread_name_prefix="thumb"
        )
        self._stop = threading.Event()
        self._ticker = threading.Thread(
            target=self._ticker_loop, name="thumb-ticker", daemon=True
        )
        self._ticker.start()

    # -- lifecycle ---------------------------------------------------------

    def shutdown(self) -> None:
        self._stop.set()
        self._executor.shutdown(wait=False, cancel_futures=True)
        # Best effort: worker threads are daemons; remove what is still here.
        self.staging.discard_all()

    def _ticker_loop(self) -> None:
        while not self._stop.wait(self.TICK_SECONDS):
            with self._lock:
                self._dispatch_locked()

    # -- ingestion: chunked staging ---------------------------------------

    def begin_upload(self) -> str:
        """Open a private staging session for one multipart upload request."""
        with self._lock:
            session_id, directory = self.staging.new_session()
            self._uploads[session_id] = {"dir": directory, "files": {}}
            return session_id

    def stage_chunk(self, session_id: str, index: int, filename: str, chunk: bytes) -> None:
        """Append one chunk of file ``index`` to its staged partial.

        The size limit is enforced while streaming, before the chunk is
        written, so an oversized file cannot fill the disk first.
        """
        with self._lock:
            upload = self._uploads.get(session_id)
            if upload is None:
                raise JobRejected("upload session is not open")
            staged = upload["files"].get(index)
            if staged is None:
                partial = self.staging.partial_path(upload["dir"], index)
                try:
                    # Module-level seam so tests can simulate write failures.
                    handle = _open(partial, "wb")
                except OSError as exc:
                    raise JobRejected(f"{filename}: cannot stage upload ({exc})") from exc
                staged = _StagedFile(
                    filename=filename, partial_path=partial, handle=handle, size=0
                )
                upload["files"][index] = staged

        limit = self.settings.max_upload_bytes
        if limit and staged.size + len(chunk) > limit:
            raise JobRejected(
                f"{filename}: upload would exceed limit of {limit} bytes"
            )
        try:
            staged.handle.write(chunk)
        except OSError as exc:
            raise JobRejected(f"{filename}: failed to write staged file ({exc})") from exc
        staged.size += len(chunk)

    def finish_file(self, session_id: str, index: int, filename: str) -> None:
        """Close and validate one staged file while the batch stays unregistered."""
        with self._lock:
            upload = self._uploads.get(session_id)
            if upload is None:
                raise JobRejected("upload session is not open")
            staged = upload["files"].get(index)
            if staged is None:
                # No chunk ever arrived: create an empty partial so the same
                # validation path reports the empty-file rejection.
                partial = self.staging.partial_path(upload["dir"], index)
                _open(partial, "wb").close()
                staged = _StagedFile(
                    filename=filename, partial_path=partial, handle=None, size=0
                )
                upload["files"][index] = staged

        if staged.handle is not None and not staged.finalized:
            try:
                staged.handle.flush()
                staged.handle.close()
            except OSError as exc:
                raise JobRejected(f"{filename}: failed to flush staged file ({exc})") from exc
            staged.finalized = True
        self._validate_staged(staged)

    def commit_upload(self, session_id: str, filenames: List[str]) -> Batch:
        """Register and dispatch the batch after all files passed validation."""
        with self._lock:
            upload = self._uploads.pop(session_id, None)
            if upload is None:
                raise JobRejected("upload session is not open")
            directory = upload["dir"]
            files = upload["files"]
            for index, filename in enumerate(filenames):
                staged = files.get(index)
                if staged is None or not staged.finalized:
                    raise JobRejected(f"{filename}: upload was not finalized")

            batch = Batch(batch_id=uuid.uuid4().hex, created_at=time.time())
            job_ids = [secrets.token_hex(8) for _ in filenames]
            try:
                paths = self.staging.commit(directory, batch.batch_id, job_ids)
            except OSError as exc:
                self.staging.discard_session(directory)
                raise JobRejected(f"failed to publish staged upload ({exc})") from exc

            for (_, filename), job_id in zip(enumerate(filenames), job_ids):
                job = Job(
                    job_id=job_id,
                    filename=filename,
                    source_path=paths[job_id],
                )
                batch.job_ids.append(job_id)
                self._jobs[job_id] = job
            self._batches[batch.batch_id] = batch
            self._dispatch_locked()
        return batch

    def abort_upload(self, session_id: str) -> None:
        """Reject the whole batch: close handles and delete all staged files."""
        with self._lock:
            upload = self._uploads.pop(session_id, None)
        if upload is None:
            return
        for staged in upload["files"].values():
            if staged.handle is not None and not staged.finalized:
                try:
                    staged.handle.close()
                except OSError:
                    pass
        self.staging.discard_session(upload["dir"])

    def create_batch(self, uploads: List[Tuple[str, bytes]]) -> Batch:
        """In-memory convenience wrapper used by tests and local callers.

        Data is still streamed through the same chunked staging pipeline (in
        ``STAGING_CHUNK_BYTES`` pieces) instead of being registered directly.
        """
        session_id = self.begin_upload()
        try:
            filenames: List[str] = []
            for index, (filename, data) in enumerate(uploads):
                filenames.append(filename)
                for start in range(0, len(data), STAGING_CHUNK_BYTES):
                    self.stage_chunk(
                        session_id, index, filename, data[start:start + STAGING_CHUNK_BYTES]
                    )
                self.finish_file(session_id, index, filename)
            return self.commit_upload(session_id, filenames)
        except JobRejected:
            self.abort_upload(session_id)
            raise

    def _validate_staged(self, staged: _StagedFile) -> None:
        filename, path, size = staged.filename, staged.partial_path, staged.size
        if size == 0:
            raise JobRejected(f"{filename}: empty file")
        limit = self.settings.max_upload_bytes
        if limit and size > limit:
            raise JobRejected(f"{filename}: {size} bytes exceeds limit of {limit}")
        try:
            with Image.open(path) as probe:
                probe.verify()
            with Image.open(path) as probe:
                if probe.format != "PNG":
                    raise JobRejected(f"{filename}: not a PNG image")
                probe.load()
        except JobRejected:
            raise
        except (UnidentifiedImageError, OSError, SyntaxError, ValueError) as exc:
            raise JobRejected(f"{filename}: invalid PNG ({exc})") from exc

    # -- scheduling --------------------------------------------------------

    def _dispatch_locked(self) -> None:
        while self._in_flight < self.settings.slot_count:
            job, kind = self._pick_next_locked()
            if job is None:
                return
            self._start_attempt_locked(job, kind)

    def _pick_next_locked(self) -> Tuple[Optional[Job], Optional[AttemptKind]]:
        # 1) Never-executed jobs win every free slot, oldest first.
        for job in self._jobs.values():
            if job.state is JobState.PENDING:
                return job, AttemptKind.FIRST

        # 2) Only with nothing pending: one make-up attempt for a first
        #    attempt that is still running past the threshold.
        now = time.time()
        for job in self._jobs.values():
            if job.state is not JobState.RUNNING or job.retry_dispatched:
                continue
            first = job.attempts[0]
            if first.state is AttemptState.RUNNING and (
                now - first.started_at >= self.settings.retry_after_seconds
            ):
                return job, AttemptKind.RETRY
        return None, None

    def _start_attempt_locked(self, job: Job, kind: AttemptKind) -> None:
        attempt = Attempt(kind=kind, started_at=time.time())
        job.attempts.append(attempt)
        job.state = JobState.RUNNING if kind is AttemptKind.FIRST else JobState.RETRYING
        self._in_flight += 1
        self._executor.submit(self._run_attempt, job.job_id, attempt)

    # -- execution ---------------------------------------------------------

    def _run_attempt(self, job_id: str, attempt: Attempt) -> None:
        job = self._jobs[job_id]
        source = job.source_path
        try:
            if not source or not os.path.exists(source):
                raise RuntimeError("staged original is unavailable")
            # Both the first and the make-up attempt read the same staged
            # file; nothing is passed in memory between requests and workers.
            png, size = self._render(source, self.settings.max_edge)
        except Exception as exc:  # noqa: BLE001 - worker thread must never die
            self._finish_attempt(
                job_id, attempt, result=None, size=None,
                error=f"{type(exc).__name__}: {exc}",
            )
        else:
            self._finish_attempt(job_id, attempt, result=png, size=size, error=None)

    def _finish_attempt(
        self,
        job_id: str,
        attempt: Attempt,
        *,
        result: Optional[bytes],
        size: Optional[Tuple[int, int]],
        error: Optional[str],
    ) -> None:
        with self._lock:
            attempt.finished_at = time.time()
            job = self._jobs[job_id]
            if result is not None:
                attempt.state = AttemptState.SUCCEEDED
                if job.state not in (JobState.SUCCEEDED, JobState.FAILED):
                    # First success wins; a later success is discarded.
                    job.state = JobState.SUCCEEDED
                    job.result = result
                    job.result_attempt = attempt.kind
                    job.result_size = size
                    job.error = None
            else:
                attempt.state = AttemptState.FAILED
                attempt.error = error
                if (
                    job.state in (JobState.RUNNING, JobState.RETRYING)
                    and not job.has_running_attempt
                ):
                    # No attempt left for this job: it has exhausted its
                    # entitlement, so it really is a failure.  (A retried job
                    # only reaches this once both attempts have failed.)
                    job.state = JobState.FAILED
                    job.error = _summarize_failures(job)
                elif job.state is JobState.RETRYING and not any(
                    a.kind is AttemptKind.RETRY and a.state is AttemptState.RUNNING
                    for a in job.attempts
                ):
                    # Make-up attempt is gone; only the original is left.
                    job.state = JobState.RUNNING

            if not job.has_running_attempt:
                # Last execution copy (success or failure) has returned: the
                # staged original is no longer needed.  The delivered
                # thumbnail bytes are kept regardless.
                source = job.source_path
                job.source_path = None
                if source is not None:
                    self.staging.release(source)
            self._in_flight -= 1
            self._dispatch_locked()

    # -- queries -----------------------------------------------------------

    def get_batch(self, batch_id: str) -> Optional[dict]:
        with self._lock:
            batch = self._batches.get(batch_id)
            if batch is None:
                return None
            jobs = [self._job_summary(self._jobs[jid]) for jid in batch.job_ids]
        return {
            "batch_id": batch.batch_id,
            "created_at": _iso(batch.created_at),
            "total": len(jobs),
            "finished": sum(
                1 for j in jobs if j["state"] in (JobState.SUCCEEDED.value, JobState.FAILED.value)
            ),
            "jobs": jobs,
        }

    def get_job(self, job_id: str) -> Optional[dict]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            return self._job_summary(job)

    def get_result(self, job_id: str) -> Optional[Tuple[Job, bytes]]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.result is None:
                return None
            return job, job.result

    def stats(self) -> dict:
        with self._lock:
            counts: Dict[str, int] = {}
            for job in self._jobs.values():
                counts[job.state.value] = counts.get(job.state.value, 0) + 1
            return {
                "slot_count": self.settings.slot_count,
                "in_flight": self._in_flight,
                "free_slots": self.settings.slot_count - self._in_flight,
                "retry_after_seconds": self.settings.retry_after_seconds,
                "max_edge": self.settings.max_edge,
                "staging_dir": self.staging.root,
                "jobs": counts,
            }

    def _job_summary(self, job: Job) -> dict:
        ready = job.result is not None
        return {
            "job_id": job.job_id,
            "filename": job.filename,
            "state": job.state.value,
            "result_ready": ready,
            "attempts": [a.as_dict() for a in job.attempts],
            "result": None if not ready else {
                "origin": job.result_attempt.value if job.result_attempt else None,
                "width": job.result_size[0] if job.result_size else None,
                "height": job.result_size[1] if job.result_size else None,
                "download_url": f"/jobs/{job.job_id}/thumbnail",
            },
            "error": job.error,
        }


def _summarize_failures(job: Job) -> str:
    errors = [a.error for a in job.attempts if a.error]
    return "; ".join(errors) if errors else "unknown failure"


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------

def create_app(
    settings: Optional[Settings] = None,
    renderer: Optional[Callable[[str, int], Tuple[bytes, Tuple[int, int]]]] = None,
    staging: Optional[StagingStore] = None,
) -> FastAPI:
    settings = settings or Settings()
    service = ThumbnailService(settings, renderer=renderer, staging=staging)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.service = service
        app.state.settings = settings
        try:
            yield
        finally:
            service.shutdown()

    app = FastAPI(title="Aerial Thumbnail Service", version="1.0.0", lifespan=lifespan)

    @app.get("/healthz")
    def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/stats")
    def stats() -> dict:
        return service.stats()

    @app.post("/batches", status_code=201)
    async def upload_batch(
        files: List[UploadFile] = File(..., description="One or more PNG files"),
    ) -> JSONResponse:
        if not files:
            raise HTTPException(status_code=400, detail="no files uploaded")

        session_id = service.begin_upload()
        filenames: List[str] = []
        try:
            for index, upload in enumerate(files):
                filename = upload.filename or "unnamed.png"
                filenames.append(filename)
                try:
                    # Drain the multipart spool in fixed-size chunks straight
                    # onto disk: the batch's originals are never accumulated
                    # in memory.  Blocking writes/validation run in worker
                    # threads so the event loop stays responsive.
                    while True:
                        chunk = await upload.read(STAGING_CHUNK_BYTES)
                        if not chunk:
                            break
                        await asyncio.to_thread(
                            service.stage_chunk, session_id, index, filename, chunk
                        )
                    await asyncio.to_thread(
                        service.finish_file, session_id, index, filename
                    )
                finally:
                    await upload.close()
            # All files are on disk and validated; only now register/dispatch.
            batch = await asyncio.to_thread(service.commit_upload, session_id, filenames)
        except JobRejected as exc:
            # Bad image, oversized file or write failure: refuse the whole
            # batch and remove every partial file this request left behind.
            await asyncio.to_thread(service.abort_upload, session_id)
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        snapshot = service.get_batch(batch.batch_id)
        assert snapshot is not None
        return JSONResponse(status_code=201, content=snapshot)

    @app.get("/batches/{batch_id}")
    def get_batch(batch_id: str) -> dict:
        snapshot = service.get_batch(batch_id)
        if snapshot is None:
            raise HTTPException(status_code=404, detail="batch not found")
        return snapshot

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str) -> dict:
        snapshot = service.get_job(job_id)
        if snapshot is None:
            raise HTTPException(status_code=404, detail="job not found")
        return snapshot

    @app.get("/jobs/{job_id}/thumbnail")
    def get_thumbnail(job_id: str) -> Response:
        job = service.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        found = service.get_result(job_id)
        if found is None:
            raise HTTPException(
                status_code=409,
                detail=f"thumbnail not ready (state={job['state']})",
            )
        result_job, data = found
        origin = result_job.result_attempt.value if result_job.result_attempt else "unknown"
        return Response(
            content=data,
            media_type="image/png",
            headers={
                # Which execution produced this deliverable.
                "X-Thumbnail-Origin": origin,
                "X-Thumbnail-Job-Id": result_job.job_id,
                "Content-Disposition": f'inline; filename="thumb-{result_job.job_id}.png"',
            },
        )

    return app


# Module-level ASGI app for ``uvicorn app.main:app``.
app = create_app()
