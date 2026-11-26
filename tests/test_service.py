import io
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
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._plans: Dict[str, List[CallPlan]] = {}
        self._calls: Dict[str, int] = {}
        self.active = 0
        self.max_active = 0
        self.total_calls = 0

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

    def __call__(self, data: bytes, max_edge: int):
        with Image.open(io.BytesIO(data)) as img:
            key = img.text["key"]
        with self._lock:
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


def make_service(renderer, slots=2, retry_after=10.0):
    return ThumbnailService(
        Settings(slot_count=slots, retry_after_seconds=retry_after, max_edge=256),
        renderer=renderer,
    )


def release_all(plans: Dict[str, List[CallPlan]]) -> None:
    for call_plans in plans.values():
        for plan in call_plans:
            plan.gate.set()


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def test_render_scales_long_edge_to_256_preserving_aspect():
    png, size = render_thumbnail(make_png("x", size=(640, 480)), 256)
    assert size == (256, 192)
    with Image.open(io.BytesIO(png)) as img:
        assert img.format == "PNG"
        assert img.size == (256, 192)


def test_render_portrait_and_no_upscale():
    assert render_thumbnail(make_png("p", size=(300, 600)), 256)[1] == (128, 256)
    assert render_thumbnail(make_png("s", size=(100, 50)), 256)[1] == (100, 50)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def test_rejects_non_png_and_keeps_batch_atomic():
    renderer = FakeRenderer()
    service = make_service(renderer)
    try:
        with pytest.raises(JobRejected):
            service.create_batch([("ok.png", make_png("ok")), ("bad.png", b"not an image")])
        assert service.stats()["jobs"] == {}
    finally:
        service.shutdown()


# ---------------------------------------------------------------------------
# Scheduling
# ---------------------------------------------------------------------------

def test_pending_images_are_dispatched_before_retries():
    """A freed slot goes to a never-started image, not to an overdue retry."""
    renderer = FakeRenderer()
    plans = {k: renderer.plan(k, 2) for k in ("slow", "second", "queued")}
    service = make_service(renderer, slots=2, retry_after=0.05)
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


def test_retry_result_downloadable_while_first_still_holds_slot():
    renderer = FakeRenderer()
    plans = {"a": renderer.plan("a", 2)}
    service = make_service(renderer, slots=3, retry_after=0.05)
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


def test_first_result_wins_and_retry_still_holds_slot():
    renderer = FakeRenderer()
    plans = {"a": renderer.plan("a", 2)}
    service = make_service(renderer, slots=3, retry_after=0.05)
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


def test_retried_job_fails_only_after_both_attempts_fail():
    renderer = FakeRenderer()
    plans = {"a": renderer.plan("a", 2)}
    service = make_service(renderer, slots=3, retry_after=0.05)
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


def test_retry_is_dispatched_at_most_once():
    renderer = FakeRenderer()
    plans = {"a": renderer.plan("a", 3)}
    service = make_service(renderer, slots=3, retry_after=0.05)
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


def test_unretried_failure_reports_immediately():
    renderer = FakeRenderer()
    plans = {"a": renderer.plan("a", 1)}
    service = make_service(renderer, slots=2, retry_after=10.0)
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


def test_global_slot_limit_never_exceeded():
    renderer = FakeRenderer()
    keys = [f"k{i}" for i in range(6)]
    plans = {k: renderer.plan(k, 2) for k in keys}
    service = make_service(renderer, slots=2, retry_after=0.05)
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
    finally:
        release_all(plans)
        service.shutdown()
