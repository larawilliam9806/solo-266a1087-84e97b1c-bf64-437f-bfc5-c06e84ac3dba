import io
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List

import pytest
from PIL import Image, PngImagePlugin

from app.main import (
    AttemptKind,
    AttemptState,
    JobRejected,
    JobState,
    Settings,
    StagingStore,
    ThumbnailService,
    render_thumbnail,
)


def make_png(key: str, size=(40, 30), color=(10, 20, 30)) -> bytes:
    img = Image.new("RGB", size, color)
    info = PngImagePlugin.PngInfo()
    info.add_text("key", key)
    out = io.BytesIO()
    img.save(out, format="PNG", pnginfo=info)
    return out.getvalue()


def write_png(tmp_path, name: str, data: bytes) -> str:
    path = os.path.join(str(tmp_path), name)
    with open(path, "wb") as handle:
        handle.write(data)
    return path


@dataclass
class CallPlan:
    started: threading.Event = field(default_factory=threading.Event)
    gate: threading.Event = field(default_factory=threading.Event)
    fail: bool = False


class FakeRenderer:
    """Deterministic renderer driven by per-key call plans.

    Each key has a list of plans, one per attempt that reaches the renderer.
    The renderer signals ``started`` and then blocks on ``gate`` until the test
    releases it (or times out so a bug cannot hang the suite).

    Renderers now receive the *staged file path*; the key metadata is read
    from that same on-disk original for every attempt.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._plans: Dict[str, List[CallPlan]] = {}
        self._calls: Dict[str, int] = {}
        self.active = 0
        self.max_active = 0
        self.total_calls = 0
        # Every staged path the renderer was asked to open.
        self.seen_paths: List[str] = []

    def plan(self, key: str, n: int) -> List[CallPlan]:
        plans = [CallPlan() for _ in range(n)]
        with self._lock:
            self._plans[key] = plans
        return plans

    def call_count(self, key: str) -> int:
        with self._lock:
            return self._calls.get(key, 0)

    def set_plan_count_if_missing(self, key: str, n: int) -> None:
        with self._lock:
            self._plans.setdefault(key, [CallPlan() for _ in range(n)])

    def __call__(self, source_path: str, max_edge: int):
        with Image.open(source_path) as img:
            key = img.text["key"]
        with self._lock:
            self.seen_paths.append(source_path)
            index = self._calls.get(key, 0)
            self._calls[key] = index + 1
            self.total_calls += 1
            plans = self._plans.setdefault(key, [])
            plans.extend(CallPlan() for _ in range(index + 1 - len(plans)))
            plan = plans[index]
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            plan.started.set()
            if not plan.gate.wait(timeout=15):
                raise TimeoutError(f"gate for {key}[{index}] never opened")
            if plan.fail:
                raise RuntimeError(f"render failed for {key}[{index}]")
            return b"thumb-" + key.encode(), (max_edge, max_edge)
        finally:
            with self._lock:
                self.active -= 1


def wait_until(predicate, timeout: float = 5.0, interval: float = 0.01):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    raise AssertionError("condition not met before timeout")


def make_service(renderer, tmp_path, slots=2, retry_after=10.0, max_upload_bytes=0):
    return ThumbnailService(
        Settings(
            slot_count=slots,
            retry_after_seconds=retry_after,
            max_edge=256,
            max_upload_bytes=max_upload_bytes,
            staging_dir=os.path.join(str(tmp_path), "staging"),
        ),
        renderer=renderer,
    )


def release_all(plans: Dict[str, List[CallPlan]]) -> None:
    for call_plans in plans.values():
        for plan in call_plans:
            plan.gate.set()


def list_staging_files(root: str) -> List[str]:
    found: List[str] = []
    for base, _dirs, names in os.walk(root):
        for name in names:
            found.append(os.path.join(base, name))
    return found


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def test_render_scales_long_edge_to_256_preserving_aspect(tmp_path):
    path = write_png(tmp_path, "x.png", make_png("x", size=(640, 480)))
    png, size = render_thumbnail(path, 256)
    assert size == (256, 192)
    with Image.open(io.BytesIO(png)) as img:
        assert img.format == "PNG"
        assert img.size == (256, 192)


def test_render_portrait_and_no_upscale(tmp_path):
    portrait = write_png(tmp_path, "p.png", make_png("p", size=(300, 600)))
    small = write_png(tmp_path, "s.png", make_png("s", size=(100, 50)))
    assert render_thumbnail(portrait, 256)[1] == (128, 256)
    assert render_thumbnail(small, 256)[1] == (100, 50)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def test_rejects_non_png_and_keeps_batch_atomic(tmp_path):
    renderer = FakeRenderer()
    service = make_service(renderer, tmp_path)
    try:
        with pytest.raises(JobRejected):
            service.create_batch([("ok.png", make_png("ok")), ("bad.png", b"not an image")])
        assert service.stats()["jobs"] == {}
        # No staged leftovers after a rejected batch.
        assert list_staging_files(service.staging.root) == []
    finally:
        service.shutdown()


def test_rejects_empty_file_and_cleans_staging(tmp_path):
    renderer = FakeRenderer()
    service = make_service(renderer, tmp_path)
    try:
        with pytest.raises(JobRejected):
            service.create_batch([("empty.png", b"")])
        assert list_staging_files(service.staging.root) == []
        assert service.stats()["jobs"] == {}
    finally:
        service.shutdown()


def test_rejects_oversized_file_during_chunked_staging(tmp_path):
    renderer = FakeRenderer()
    service = make_service(renderer, tmp_path, max_upload_bytes=64)
    try:
        # Feed bytes through the same chunked path the HTTP handler uses.
        session_id = service.begin_upload()
        payload = make_png("big")  # well above the 64-byte cap
        assert len(payload) > 64
        try:
            with pytest.raises(JobRejected):
                for start in range(0, len(payload), 32):
                    service.stage_chunk(session_id, 0, "big.png", payload[start:start + 32])
        finally:
            service.abort_upload(session_id)
        assert list_staging_files(service.staging.root) == []
        assert service.stats()["jobs"] == {}
    finally:
        service.shutdown()


def test_disk_write_failure_rejects_and_cleans_batch(tmp_path, monkeypatch):
    renderer = FakeRenderer()
    staging = StagingStore(os.path.join(str(tmp_path), "staging"))
    service = ThumbnailService(
        Settings(staging_dir=staging.root), renderer=renderer, staging=staging
    )
    import app.main as main_module

    original_open = main_module._open

    def flaky_open(path, *args, **kwargs):
        handle = original_open(path, *args, **kwargs)
        if str(path).endswith(".part"):
            def failing_write(chunk):
                raise OSError("simulated disk failure")
            handle.write = failing_write  # type: ignore[method-assign]
        return handle

    monkeypatch.setattr(main_module, "_open", flaky_open)
    try:
        with pytest.raises(JobRejected):
            service.create_batch([("a.png", make_png("a"))])
        assert service.stats()["jobs"] == {}
        assert list_staging_files(staging.root) == []
    finally:
        service.shutdown()


def test_staging_store_sweeps_orphans_on_startup(tmp_path):
    root = os.path.join(str(tmp_path), "staging")
    first = StagingStore(root)
    _, session_dir = first.new_session()
    with open(os.path.join(session_dir, "0.part"), "wb") as handle:
        handle.write(b"orphan")
    batch_dir = os.path.join(root, "deadbeef")
    os.makedirs(batch_dir)
    with open(os.path.join(batch_dir, "cafe.png"), "wb") as handle:
        handle.write(b"orphan")

    # A fresh process: in-memory state is gone, disk leftovers are swept.
    second = StagingStore(root)
    assert list_staging_files(second.root) == []
    assert os.path.isdir(second.incoming_root)


# ---------------------------------------------------------------------------
# Staging lifecycle around attempts
# ---------------------------------------------------------------------------

def test_both_attempts_read_same_staged_original(tmp_path):
    renderer = FakeRenderer()
    plans = {"a": renderer.plan("a", 2)}
    service = make_service(renderer, tmp_path, slots=3, retry_after=0.05)
    try:
        job_id = service.create_batch([("a.png", make_png("a"))]).job_ids[0]
        wait_until(lambda: renderer.call_count("a") == 2)
        job = service._jobs[job_id]
        staged = job.source_path
        assert staged is not None and os.path.exists(staged)
        # First and retry opened the identical file reference.
        assert renderer.seen_paths == [staged, staged]
        release_all(plans)
        wait_until(lambda: service.stats()["in_flight"] == 0)
    finally:
        release_all(plans)
        service.shutdown()


def test_original_kept_while_retry_runs_and_deleted_after_last_attempt(tmp_path):
    renderer = FakeRenderer()
    plans = {"a": renderer.plan("a", 2)}
    service = make_service(renderer, tmp_path, slots=3, retry_after=0.05)
    try:
        job_id = service.create_batch([("a.png", make_png("a"))]).job_ids[0]
        wait_until(lambda: renderer.call_count("a") == 2)

        plans["a"][1].gate.set()  # retry finishes first, original keeps running
        wait_until(lambda: service.get_job(job_id)["state"] == JobState.SUCCEEDED.value)
        job = service._jobs[job_id]
        # Deliverable is ready but the original must survive for the slow copy.
        assert job.result is not None
        assert job.source_path is not None
        assert os.path.exists(job.source_path)

        plans["a"][0].gate.set()  # last attempt returns -> original is cleaned
        wait_until(lambda: service.stats()["in_flight"] == 0)
        assert service._jobs[job_id].source_path is None
        assert list_staging_files(service.staging.root) == []
        # Thumbnail stays downloadable after the original is gone.
        assert service.get_result(job_id)[1] == b"thumb-a"
    finally:
        release_all(plans)
        service.shutdown()


def test_original_cleaned_when_all_attempts_fail(tmp_path):
    renderer = FakeRenderer()
    plans = {"a": renderer.plan("a", 2)}
    service = make_service(renderer, tmp_path, slots=3, retry_after=0.05)
    try:
        job_id = service.create_batch([("a.png", make_png("a"))]).job_ids[0]
        wait_until(lambda: renderer.call_count("a") == 2)
        plans["a"][0].fail = True
        plans["a"][1].fail = True
        release_all(plans)
        wait_until(lambda: service.get_job(job_id)["state"] == JobState.FAILED.value)
        assert service._jobs[job_id].source_path is None
        assert list_staging_files(service.staging.root) == []
        assert service.get_result(job_id) is None
    finally:
        release_all(plans)
        service.shutdown()


def test_abort_upload_removes_session_directory(tmp_path):
    renderer = FakeRenderer()
    service = make_service(renderer, tmp_path)
    try:
        session_id = service.begin_upload()
        service.stage_chunk(session_id, 0, "a.png", make_png("a"))
        upload = service._uploads[session_id]
        assert os.path.isdir(upload["dir"])
        service.abort_upload(session_id)
        assert not os.path.exists(upload["dir"])
        assert session_id not in service._uploads
    finally:
        service.shutdown()


def test_committed_batch_lives_under_batch_directory(tmp_path):
    renderer = FakeRenderer()
    service = make_service(renderer, tmp_path)
    try:
        batch = service.create_batch(
            [("a.png", make_png("a")), ("b.png", make_png("b"))]
        )
        batch_dir = os.path.join(service.staging.root, batch.batch_id)
        assert sorted(os.listdir(batch_dir)) == sorted(
            [f"{job_id}.png" for job_id in batch.job_ids]
        )
        # Nothing lingers in the incoming area.
        assert os.listdir(service.staging.incoming_root) == []
    finally:
        service.shutdown()


# ---------------------------------------------------------------------------
# Scheduling
# ---------------------------------------------------------------------------

def test_pending_images_are_dispatched_before_retries(tmp_path):
    """A freed slot goes to a never-started image, not to an overdue retry."""
    renderer = FakeRenderer()
    plans = {k: renderer.plan(k, 2) for k in ("slow", "second", "queued")}
    service = make_service(renderer, tmp_path, slots=2, retry_after=0.05)
    try:
        batch = service.create_batch(
            [("slow.png", make_png("slow")), ("second.png", make_png("second"))]
        )
        wait_until(lambda: plans["slow"][0].started.is_set() and plans["second"][0].started.is_set())
        assert service.stats()["in_flight"] == 2

        queued_batch = service.create_batch([("queued.png", make_png("queued"))])
        queued_id = queued_batch.job_ids[0]
        assert service.get_job(queued_id)["state"] == JobState.PENDING.value

        # "slow" becomes overdue, but no slot is free.
        time.sleep(0.2)
        assert renderer.call_count("slow") == 1

        # Freeing a slot must serve "queued" first even though "slow" is overdue.
        plans["second"][0].gate.set()
        wait_until(lambda: plans["queued"][0].started.is_set())
        assert renderer.call_count("slow") == 1
        assert service.get_job(queued_id)["state"] == JobState.RUNNING.value

        # Only once nothing is pending does "slow" get its one make-up run.
        plans["queued"][0].gate.set()
        wait_until(lambda: renderer.call_count("slow") == 2)
        slow_job = service.get_job(batch.job_ids[0])
        assert [a["kind"] for a in slow_job["attempts"]] == ["first", "retry"]
    finally:
        release_all(plans)
        service.shutdown()


def test_retry_result_downloadable_while_first_still_holds_slot(tmp_path):
    renderer = FakeRenderer()
    plans = {"a": renderer.plan("a", 2)}
    service = make_service(renderer, tmp_path, slots=3, retry_after=0.05)
    try:
        job_id = service.create_batch([("a.png", make_png("a"))]).job_ids[0]
        wait_until(lambda: plans["a"][0].started.is_set())
        wait_until(lambda: renderer.call_count("a") == 2)

        plans["a"][1].gate.set()  # make-up attempt succeeds, first still running
        wait_until(lambda: service.get_job(job_id)["state"] == JobState.SUCCEEDED.value)
        job = service.get_job(job_id)
        assert job["result_ready"] is True
        assert job["result"]["origin"] == AttemptKind.RETRY.value
        assert service.get_result(job_id)[1] == b"thumb-a"

        # The finished copy's slot stays occupied until the slow copy returns.
        assert service.stats()["in_flight"] == 1
        assert not plans["a"][0].gate.is_set()

        plans["a"][0].gate.set()
        wait_until(lambda: service.stats()["in_flight"] == 0)
        # A late success from the original must not replace the delivered result.
        assert service.get_job(job_id)["result"]["origin"] == AttemptKind.RETRY.value
    finally:
        release_all(plans)
        service.shutdown()


def test_first_result_wins_and_retry_still_holds_slot(tmp_path):
    renderer = FakeRenderer()
    plans = {"a": renderer.plan("a", 2)}
    service = make_service(renderer, tmp_path, slots=3, retry_after=0.05)
    try:
        job_id = service.create_batch([("a.png", make_png("a"))]).job_ids[0]
        wait_until(lambda: plans["a"][0].started.is_set())
        wait_until(lambda: renderer.call_count("a") == 2)

        plans["a"][0].gate.set()  # original succeeds first
        wait_until(lambda: service.get_job(job_id)["state"] == JobState.SUCCEEDED.value)
        assert service.get_job(job_id)["result"]["origin"] == AttemptKind.FIRST.value
        assert service.stats()["in_flight"] == 1  # retry copy still occupies a slot

        plans["a"][1].gate.set()
        wait_until(lambda: service.stats()["in_flight"] == 0)
        assert service.get_job(job_id)["result"]["origin"] == AttemptKind.FIRST.value
        assert service.get_result(job_id)[1] == b"thumb-a"
    finally:
        release_all(plans)
        service.shutdown()


def test_retried_job_fails_only_after_both_attempts_fail(tmp_path):
    renderer = FakeRenderer()
    plans = {"a": renderer.plan("a", 2)}
    service = make_service(renderer, tmp_path, slots=3, retry_after=0.05)
    try:
        job_id = service.create_batch([("a.png", make_png("a"))]).job_ids[0]
        wait_until(lambda: plans["a"][0].started.is_set())
        wait_until(lambda: renderer.call_count("a") == 2)

        plans["a"][1].fail = True
        plans["a"][1].gate.set()  # make-up fails while the original still runs
        wait_until(
            lambda: service.get_job(job_id)["attempts"][1]["state"] == AttemptState.FAILED.value
        )
        job = service.get_job(job_id)
        assert job["state"] != JobState.FAILED.value
        assert job["result_ready"] is False

        plans["a"][0].fail = True
        plans["a"][0].gate.set()  # original fails too -> terminal failure
        wait_until(lambda: service.get_job(job_id)["state"] == JobState.FAILED.value)
        job = service.get_job(job_id)
        assert "render failed for a[0]" in job["error"]
        assert "render failed for a[1]" in job["error"]
    finally:
        release_all(plans)
        service.shutdown()


def test_retry_is_dispatched_at_most_once(tmp_path):
    renderer = FakeRenderer()
    plans = {"a": renderer.plan("a", 3)}
    service = make_service(renderer, tmp_path, slots=3, retry_after=0.05)
    try:
        job_id = service.create_batch([("a.png", make_png("a"))]).job_ids[0]
        wait_until(lambda: renderer.call_count("a") == 2)

        plans["a"][1].fail = True
        plans["a"][1].gate.set()
        wait_until(
            lambda: service.get_job(job_id)["attempts"][1]["state"] == AttemptState.FAILED.value
        )

        # Original is still running and overdue, yet no third attempt appears.
        time.sleep(0.3)
        assert renderer.call_count("a") == 2
        assert len(service.get_job(job_id)["attempts"]) == 2

        plans["a"][0].gate.set()
        wait_until(lambda: service.stats()["in_flight"] == 0)
    finally:
        release_all(plans)
        service.shutdown()


def test_unretried_failure_reports_immediately(tmp_path):
    renderer = FakeRenderer()
    plans = {"a": renderer.plan("a", 1)}
    service = make_service(renderer, tmp_path, slots=2, retry_after=10.0)
    try:
        job_id = service.create_batch([("a.png", make_png("a"))]).job_ids[0]
        wait_until(lambda: plans["a"][0].started.is_set())
        plans["a"][0].fail = True
        plans["a"][0].gate.set()
        wait_until(lambda: service.get_job(job_id)["state"] == JobState.FAILED.value)
        assert len(service.get_job(job_id)["attempts"]) == 1
    finally:
        release_all(plans)
        service.shutdown()


def test_global_slot_limit_never_exceeded(tmp_path):
    renderer = FakeRenderer()
    keys = [f"k{i}" for i in range(6)]
    plans = {k: renderer.plan(k, 2) for k in keys}
    service = make_service(renderer, tmp_path, slots=2, retry_after=0.05)
    try:
        batch = service.create_batch([(f"{k}.png", make_png(k)) for k in keys])
        wait_until(lambda: renderer.active == 2)
        time.sleep(0.25)  # overdue-retry logic must not over-dispatch
        assert renderer.max_active <= 2
        assert service.stats()["in_flight"] <= 2

        release_all(plans)
        wait_until(
            lambda: service.get_batch(batch.batch_id)["finished"] == len(keys), timeout=10
        )
        assert renderer.max_active <= 2
        assert service.stats()["in_flight"] == 0
        # Every original has been released once its last attempt finished.
        assert list_staging_files(service.staging.root) == []
    finally:
        release_all(plans)
        service.shutdown()
