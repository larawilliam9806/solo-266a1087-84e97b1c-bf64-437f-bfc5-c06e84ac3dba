# 航拍底图批量缩略图服务

FastAPI + Pillow 的单机服务：一次上传一批 PNG，后台把每张图等比缩放到**长边 256 像素**，
提供批号查询与缩略图下载。用 docker compose 一键启动。

## 启动

```bash
docker compose up --build
# 服务在 http://localhost:8000 ，交互文档 /docs
```

可在同目录建 `.env` 覆盖默认值（见下方"配置"）。

本地开发：

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## 配置

| 环境变量 | 默认 | 含义 |
| --- | --- | --- |
| `THUMBNAIL_SLOTS` | `4` | 全局并发槽位数：同一时刻最多有多少个执行副本在跑 |
| `THUMBNAIL_RETRY_AFTER_SECONDS` | `30` | 补跑阈值：首次执行仍在运行且超过该秒数后，才允许补跑 |
| `THUMBNAIL_MAX_EDGE` | `256` | 缩略图长边像素 |
| `THUMBNAIL_MAX_UPLOAD_BYTES` | `0` | 单文件上传上限（字节），`0` 表示不限 |
| `THUMBNAIL_PORT` | `8000` | 宿主机端口（仅 compose） |

## 接口

### 上传一批 PNG

```bash
curl -F 'files=@a.png' -F 'files=@b.png' http://localhost:8000/batches
```

返回 `201` 和批次状态（含 `batch_id` 与每个 job 的 `job_id`、`state`、`attempts`、`result`）。
任何一个文件不是合法 PNG 时整批拒绝（`400`），不会留下半批数据。

### 按批号查询

```bash
curl http://localhost:8000/batches/<batch_id>
```

```json
{
  "batch_id": "...",
  "created_at": "2026-11-26T04:54:21.100Z",
  "total": 2,
  "finished": 1,
  "jobs": [
    {
      "job_id": "693876f6dbde049f",
      "filename": "a.png",
      "state": "succeeded",
      "result_ready": true,
      "attempts": [
        {"kind": "first", "state": "running",    "started_at": "...", "finished_at": null, "error": null},
        {"kind": "retry", "state": "succeeded",  "started_at": "...", "finished_at": "...", "error": null}
      ],
      "result": {
        "origin": "retry",
        "width": 256,
        "height": 192,
        "download_url": "/jobs/693876f6dbde049f/thumbnail"
      },
      "error": null
    }
  ]
}
```

`state` 取值：`pending`（排队未执行）/ `running`（首次执行中）/ `retrying`（首次未结束、补跑进行中）/
`succeeded` / `failed`。

### 下载缩略图

```bash
curl -OJ http://localhost:8000/jobs/<job_id>/thumbnail
```

首次有任一执行副本成功即可下载，响应头 `X-Thumbnail-Origin` 标明结果来自 `first` 还是 `retry`
（批次 JSON 中的 `result.origin` 同义）。尚未就绪返回 `409`，未知 id 返回 `404`。

### 其它

- `GET /jobs/<job_id>`：单图状态。
- `GET /stats`：槽位占用、剩余槽位、配置值与各状态任务计数。
- `GET /healthz`：健康检查（compose 已配置）。

## 调度规则

1. **槽位**：全局共 `THUMBNAIL_SLOTS` 个，一个执行副本占一个。槽位只在副本**真正结束**时归还——
   结果已可下载但另一个副本仍在跑时，那个副本继续占着槽位。
2. **派发优先级**：有空位时先给从未执行过的图片（FIFO）；所有等待中的图片都拿到槽位后，
   才轮到补跑。
3. **补跑**：仅当首次执行**仍在运行**且已超过 `THUMBNAIL_RETRY_AFTER_SECONDS`，才并发再跑一次；
   每张图最多补跑一次。首次执行已结束（成功或失败）的图不会补跑。
4. **交付**：最先成功的副本胜出，立即开放下载并标注来源；之后完成的副本结果直接丢弃，
   不覆盖已交付结果。
5. **失败判定**：只有在"该任务应得的全部执行都失败"后才报 `failed`。补跑过的任务需两次都失败；
   未补跑的任务单次失败即为 `failed`（它已不可能再补跑，因为首次执行已经结束）。

## 测试

```bash
pip install -r requirements.txt pytest httpx
python -m pytest -q
```

测试用可控的假渲染器精确驱动慢任务/失败场景，覆盖优先级、补跑唯一性、双失败判定、
"结果可下载但副本仍占槽位"以及并发不超限。

## 说明与边界

- 调度状态在内存中，**单进程**运行（compose 固定单副本，请勿加 `--workers`）；重启会丢批次。
- 上传文件先完整读入内存校验，再交给工作线程渲染。面向制图组的内网服务场景，
  如需处理超大底图请调 `THUMBNAIL_MAX_UPLOAD_BYTES` 或改为落盘暂存。
- 小图不放大：长边小于 256 的图保持原尺寸（"缩放到长边 256" 按"不超过 256 的等比缩放"理解）。
- 慢副本无法被强制中断（Python 线程限制），所以迟到的结果只做丢弃，槽位等它自然结束。
