import io
import os
import threading
import time

import pytest
from fastapi.testclient import TestClient
from PIL import Image, PngImagePlugin

from app.main import (
    STAGING_CHUNK_BYTES,
    AttemptState,
    JobState,
    Settings,
    create_app,
    render_thumbnail,
)


def make_png(size=(320, 240), color=(200, 100, 50)) -> bytes:
    out = io.BytesIO()
    Image.new("RGB", size, color).save(out, format="PNG")
    return out.getvalue()


def png_with_key(key: str, size=(320, 240)) -> bytes:
    info = PngImagePlugin.PngInfo()
    info.add_text("key", key)
    out = io.BytesIO()
    Image.new("RGB", size, (1, 2, 3)).save(out, format="PNG", pnginfo=info)
    return out.getvalue()


def list_staging_files(root: str):
    found = []
    for base, _dirs, names in os.walk(root):
        for name in names:
            found.append(os.path.join(base, name))
    return found


@pytest.fixture
def client(tmp_path):
    app = create_app(
        Settings(
            slot_count=2,
            retry_after_seconds=30.0,
            max_edge=256,
            staging_dir=os.path.join(str(tmp_path), "staging"),
        )
    )
    with TestClient(app) as test_client:
        yield test_client


def wait_until(predicate, timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition not met before timeout")


# ---------------------------------------------------------------------------
# Upload + status + download
# ---------------------------------------------------------------------------

def test_upload_returns_batch_and_jobs_become_ready(client):
    resp = client.post(
        "/batches",
        files=[
            ("files", ("a.png", make_png((640, 480)), "image/png")),
            ("files", ("b.png", make_png((300, 600)), "image/png")),
        ],
    )
    assert resp.status_code == 201, resp.text
    batch = resp.json()
    assert batch["total"] == 2
    assert len(batch["jobs"]) == 2
    batch_id = batch["batch_id"]

    wait_until(lambda: client.get(f"/batches/{batch_id}").json()["finished"] == 2)
    status = client.get(f"/batches/{batch_id}").json()
    by_name = {j["filename"]: j for j in status["jobs"]}
    assert by_name["a.png"]["result"]["width"] == 256
    assert by_name["a.png"]["result"]["height"] == 192
    assert by_name["b.png"]["result"]["width"] == 128
    assert by_name["b.png"]["result"]["height"] == 256

    job = by_name["a.png"]
    thumb = client.get(job["result"]["download_url"])
    assert thumb.status_code == 200
    assert thumb.headers["content-type"] == "image/png"
    assert thumb.headers["x-thumbnail-origin"] == "first"
    with Image.open(io.BytesIO(thumb.content)) as img:
        assert img.size == (256, 192)

    job_detail = client.get(f"/jobs/{job['job_id']}").json()
    assert job_detail["result"]["origin"] == "first"


def test_batch_and_job_not_found(client):
    assert client.get("/batches/does-not-exist").status_code == 404
    assert client.get("/jobs/does-not-exist").status_code == 404
    assert client.get("/jobs/does-not-exist/thumbnail").status_code == 404


def test_upload_rejects_invalid_file(client, tmp_path):
    staging_root = os.path.join(str(tmp_path), "staging")
    resp = client.post(
        "/batches",
        files=[("files", ("bad.png", b"definitely not a png", "image/png"))],
    )
    assert resp.status_code == 400
    assert "bad.png" in resp.json()["detail"]
    # Whole batch refused: every partial staged file is cleaned up.
    assert list_staging_files(staging_root) == []


def test_upload_rejects_empty_file_and_cleans_staging(client, tmp_path):
    staging_root = os.path.join(str(tmp_path), "staging")
    resp = client.post(
        "/batches",
        files=[("files", ("empty.png", b"", "image/png"))],
    )
    assert resp.status_code == 400
    assert "empty" in resp.json()["detail"]
    assert list_staging_files(staging_root) == []


def test_upload_rejects_oversized_file_and_cleans_staging(tmp_path):
    staging_root = os.path.join(str(tmp_path), "staging")
    app = create_app(
        Settings(
            slot_count=1,
            retry_after_seconds=30.0,
            max_edge=256,
            max_upload_bytes=128,
            staging_dir=staging_root,
        )
    )
    with TestClient(app) as small_client:
        resp = small_client.post(
            "/batches",
            files=[("files", ("big.png", make_png((640, 480)), "image/png"))],
        )
        assert resp.status_code == 400
        assert "big.png" in resp.json()["detail"]
        assert "limit" in resp.json()["detail"]
        assert list_staging_files(staging_root) == []


def test_rejected_batch_leaves_no_registered_jobs(client):
    resp = client.post(
        "/batches",
        files=[
            ("files", ("ok.png", make_png((40, 30)), "image/png")),
            ("files", ("bad.png", b"not a png", "image/png")),
        ],
    )
    assert resp.status_code == 400
    stats = client.get("/stats").json()
    assert stats["jobs"] == {}


def test_large_upload_is_chunked_onto_disk_and_cleaned_afterwards(client, tmp_path):
    # A payload spanning multiple read chunks must still succeed while never
    # being assembled into one in-memory blob.  Noisy pixels keep the PNG
    # incompressible so it genuinely exceeds the 1 MiB streaming chunk.
    noisy = Image.effect_noise((1000, 1000), 128).convert("RGB")
    out = io.BytesIO()
    noisy.save(out, format="PNG")
    payload = out.getvalue()
    assert len(payload) > STAGING_CHUNK_BYTES
    resp = client.post(
        "/batches",
        files=[("files", ("large.png", payload, "image/png"))],
    )
    assert resp.status_code == 201, resp.text
    batch_id = resp.json()["batch_id"]
    job_id = resp.json()["jobs"][0]["job_id"]

    staging_root = os.path.join(str(tmp_path), "staging")
    wait_until(lambda: client.get(f"/batches/{batch_id}").json()["finished"] == 1)
    wait_until(lambda: client.get("/stats").json()["in_flight"] == 0)
    # Once the last attempt ended the batch directory is pruned entirely.
    assert not os.path.exists(os.path.join(staging_root, batch_id))
    assert list_staging_files(staging_root) == []

    # The deliverable itself is still downloadable after the original cleanup.
    thumb = client.get(f"/jobs/{job_id}/thumbnail")
    assert thumb.status_code == 200
    with Image.open(io.BytesIO(thumb.content)) as img:
        assert max(img.size) == 256


def test_upload_rejects_empty_batch(client):
    resp = client.post("/batches")
    assert resp.status_code in (400, 422)


# ---------------------------------------------------------------------------
# Slow-path behaviour over HTTP
# ---------------------------------------------------------------------------

class SlowishRenderer:
    """Renderer that stalls the *first* attempt for a chosen key only.

    The make-up attempt then finishes first, which is the case we want to
    exercise: the retry supplies the deliverable while the original is still
    running.  Renderers read the staged file from its on-disk path.
    """

    def __init__(self, slow_key: str, slow_seconds: float):
        self.slow_key = slow_key
        self.slow_seconds = slow_seconds
        self._lock = threading.Lock()
        self.calls = []
        self._slow_calls = 0

    def __call__(self, source_path: str, max_edge: int):
        with Image.open(source_path) as img:
            key = img.text["key"]
        with self._lock:
            self.calls.append((key, source_path))
            if key == self.slow_key:
                self._slow_calls += 1
                should_stall = self._slow_calls == 1
            else:
                should_stall = False
        if should_stall:
            time.sleep(self.slow_seconds)
        return render_thumbnail(source_path, max_edge)


def test_http_retry_origin_reported_and_downloadable(tmp_path):
    renderer = SlowishRenderer(slow_key="slow", slow_seconds=0.4)
    app = create_app(
        Settings(
            slot_count=4,
            retry_after_seconds=0.1,
            max_edge=256,
            staging_dir=os.path.join(str(tmp_path), "staging"),
        ),
        renderer=renderer,
    )
    with TestClient(app) as client:
        resp = client.post(
            "/batches",
            files=[("files", ("slow.png", png_with_key("slow", (640, 480)), "image/png"))],
        )
        assert resp.status_code == 201
        batch_id = resp.json()["batch_id"]
        job_id = resp.json()["jobs"][0]["job_id"]

        # The make-up attempt finishes before the slow original.
        wait_until(lambda: client.get(f"/jobs/{job_id}").json()["result_ready"])
        job = client.get(f"/jobs/{job_id}").json()
        assert job["result"]["origin"] == "retry"
        assert [a["kind"] for a in job["attempts"]] == ["first", "retry"]

        # Both copies opened the same staged original.
        paths = {path for key, path in renderer.calls if key == "slow"}
        assert paths == {os.path.join(
            os.path.join(str(tmp_path), "staging"), batch_id, f"{job_id}.png"
        )}

        thumb = client.get(f"/jobs/{job_id}/thumbnail")
        assert thumb.status_code == 200
        assert thumb.headers["x-thumbnail-origin"] == "retry"
        with Image.open(io.BytesIO(thumb.content)) as img:
            assert img.size == (256, 192)

        # Both attempts eventually return; the batch settles as a success and
        # the original is cleaned up while the thumbnail stays downloadable.
        wait_until(lambda: client.get(f"/batches/{batch_id}").json()["finished"] == 1)
        # "finished" flips at first success; wait for the slow first attempt
        # to actually return before expecting the original to be released.
        wait_until(lambda: client.get("/stats").json()["in_flight"] == 0)
        final = client.get(f"/jobs/{job_id}").json()
        assert final["state"] == JobState.SUCCEEDED.value
        assert final["attempts"][1]["state"] == AttemptState.SUCCEEDED.value
        assert list_staging_files(os.path.join(str(tmp_path), "staging")) == []
        assert client.get(f"/jobs/{job_id}/thumbnail").status_code == 200


def test_http_failed_job_cleans_original(tmp_path):
    class FailingRenderer:
        def __call__(self, source_path: str, max_edge: int):
            raise RuntimeError("boom")

    staging_root = os.path.join(str(tmp_path), "staging")
    app = create_app(
        Settings(
            slot_count=1,
            retry_after_seconds=30.0,
            max_edge=256,
            staging_dir=staging_root,
        ),
        renderer=FailingRenderer(),
    )
    with TestClient(app) as failing_client:
        resp = failing_client.post(
            "/batches",
            files=[("files", ("a.png", make_png(), "image/png"))],
        )
        assert resp.status_code == 201
        job_id = resp.json()["jobs"][0]["job_id"]
        wait_until(
            lambda: failing_client.get(f"/jobs/{job_id}").json()["state"]
            == JobState.FAILED.value
        )
        assert list_staging_files(staging_root) == []
        assert failing_client.get(f"/jobs/{job_id}/thumbnail").status_code == 409


def test_http_stats_endpoint(client):
    stats = client.get("/stats").json()
    assert stats["slot_count"] == 2
    assert stats["retry_after_seconds"] == 30.0
    assert stats["max_edge"] == 256
    assert stats["staging_dir"]


def test_http_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}
