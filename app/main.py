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

State is in-memory and process-local: run exactly one instance (the shipped
docker compose file does).
"""

from __future__ import annotations

import io
import os
import secrets
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List, Optional, Tuple

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse, Response
from PIL import Image, UnidentifiedImageError


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

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
    data: Optional[bytes]
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


class JobRejected(Exception):
    """Upload rejected before a batch was created (bad or oversized file)."""


def _iso(ts: float) -> str:
    whole = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts))
    return f"{whole}.{int((ts % 1) * 1000):03d}Z"


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render_thumbnail(data: bytes, max_edge: int) -> Tuple[bytes, Tuple[int, int]]:
    """Downscale PNG bytes to fit ``max_edge``; return (png_bytes, (w, h)).

    Aspect ratio is preserved.  Smaller images are not upscaled.
    """
    with Image.open(io.BytesIO(data)) as img:
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
        renderer: Optional[Callable[[bytes, int], Tuple[bytes, Tuple[int, int]]]] = None,
    ):
        self.settings = settings
        self._render = renderer or render_thumbnail
        self._lock = threading.RLock()
        self._batches: Dict[str, Batch] = {}
        self._jobs: Dict[str, Job] = {}
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

    def _ticker_loop(self) -> None:
        while not self._stop.wait(self.TICK_SECONDS):
            with self._lock:
                self._dispatch_locked()

    # -- ingestion ---------------------------------------------------------

    def create_batch(self, uploads: List[Tuple[str, bytes]]) -> Batch:
        """Validate every file, then register the batch atomically."""
        validated = [(name, data) for name, data in (self._validate(*u) for u in uploads)]
        batch = Batch(batch_id=uuid.uuid4().hex, created_at=time.time())
        with self._lock:
            self._batches[batch.batch_id] = batch
            for filename, data in validated:
                job = Job(job_id=secrets.token_hex(8), filename=filename, data=data)
                batch.job_ids.append(job.job_id)
                self._jobs[job.job_id] = job
            self._dispatch_locked()
        return batch

    def _validate(self, filename: str, data: bytes) -> Tuple[str, bytes]:
        if not data:
            raise JobRejected(f"{filename}: empty file")
        limit = self.settings.max_upload_bytes
        if limit and len(data) > limit:
            raise JobRejected(f"{filename}: {len(data)} bytes exceeds limit of {limit}")
        try:
            with Image.open(io.BytesIO(data)) as probe:
                probe.verify()
            with Image.open(io.BytesIO(data)) as probe:
                if probe.format != "PNG":
                    raise JobRejected(f"{filename}: not a PNG image")
                probe.load()
        except JobRejected:
            raise
        except (UnidentifiedImageError, OSError, SyntaxError, ValueError) as exc:
            raise JobRejected(f"{filename}: invalid PNG ({exc})") from exc
        return filename, data

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
        data = self._jobs[job_id].data
        try:
            if data is None:  # pragma: no cover - defensive
                raise RuntimeError("payload unavailable")
            png, size = self._render(data, self.settings.max_edge)
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
                job.data = None  # release the original payload
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
    renderer: Optional[Callable[[bytes, int], Tuple[bytes, Tuple[int, int]]]] = None,
) -> FastAPI:
    settings = settings or Settings()
    service = ThumbnailService(settings, renderer=renderer)

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
        payloads: List[Tuple[str, bytes]] = []
        for upload in files:
            try:
                data = await upload.read()
            finally:
                await upload.close()
            payloads.append((upload.filename or "unnamed.png", data))
        try:
            batch = service.create_batch(payloads)
        except JobRejected as exc:
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
