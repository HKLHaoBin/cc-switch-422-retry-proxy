# cc-switch-422-retry-proxy

A small local retry layer that sits between Claude Code / Claude Desktop and a CC Switch local route. It is a **temporary workaround** for transient upstream failures (for example Codex/Responses `422 service_unavailable_error: Our servers are currently overloaded`), until CC Switch provides a built-in, safe retry policy.

> ⚠️ **Read before use.** This proxy can **replay a request that the upstream may already have accepted**. A failed HTTP response does not prove the upstream did not receive or process the request, so a retry can cause duplicate responses, duplicate billing, or duplicate tool side effects. This is exactly the risk raised in CC Switch PR #6241. Use it only if you accept that trade-off.

中文说明：这是一个本地“重试层”，位于 Claude Code / Claude Desktop 与 CC Switch 本地路由之间，用于临时应对上游过载等瞬时错误。**注意：它可能重放上游已经接收过的请求，从而造成重复回答、重复计费或重复工具调用。** 请自行承担风险。

## What it does

- **Local `count_tokens`**: answers `POST /v1/messages/count_tokens` locally with a conservative estimate (`bytes / 3`), because CC Switch's route returns 404 for it and Claude Code may send many of these during compaction.
- **422 overload retry**: retries `422` responses whose body matches transient overload markers (`service_unavailable`, `servers are currently overloaded`, ...).
- **Connection / 502 / 503 / 504 retry** for generation requests (`POST`): retries with backoff.
- **Streaming validation**: for `stream: true` requests, the proxy buffers the upstream SSE response until it sees a terminal event (`message_stop` / `response.completed` / `[DONE]`). If it sees an SSE error event, `error decoding response body`, or a truncated stream, it discards that attempt and retries. Only a complete stream is forwarded to the client.
- **Concurrency limit** toward the target (default 16) to avoid overwhelming the local route.
- Retries use backoff `2, 4, 8, 15, 30` seconds (capped at 30 s) and **do not stop on their own**: a request is retried until it succeeds or the proxy process is stopped.

## Trade-offs

- **Duplicate requests**: see the warning above. Retries are not idempotency-aware.
- **No real-time streaming**: streamed responses are buffered in full before being sent, so the client sees no incremental output. Streams larger than `CCSWITCH_RETRY_MAX_BUFFERED_STREAM_BYTES` fall back to pass-through.
- **Unbounded retries**: a permanently failing request is retried forever with 30 s max backoff. Stop the proxy process to abort.
- **Approximate token counts**: `count_tokens` is an estimate, not the upstream tokenizer.
- Designed for a Windows setup and tested against a local CC Switch route; behavior on other setups is not verified.

## Requirements

- Python 3.10+ (standard library only, no pip dependencies).

## Setup

1. Run CC Switch as usual; its local route listens on `127.0.0.1:15721` by default.
2. Start the proxy:

   ```powershell
   python codex_422_retry_proxy.py
   ```

   It listens on `127.0.0.1:15722` and forwards to `http://127.0.0.1:15721/claude-desktop`.
3. Point your Claude client's gateway base URL at `http://127.0.0.1:15722/claude-desktop` (instead of `15721`).

## Configuration (environment variables)

| Variable | Default | Meaning |
| --- | --- | --- |
| `CCSWITCH_RETRY_HOST` | `127.0.0.1` | Listen host |
| `CCSWITCH_RETRY_PORT` | `15722` | Listen port |
| `CCSWITCH_RETRY_TARGET` | `http://127.0.0.1:15721/claude-desktop` | Upstream CC Switch route |
| `CCSWITCH_RETRY_TARGET_TIMEOUT` | `130` | Per-attempt timeout (seconds) |
| `CCSWITCH_RETRY_TARGET_MAX_CONCURRENCY` | `16` | Max concurrent requests to the target |
| `CCSWITCH_RETRY_MAX_BUFFERED_STREAM_BYTES` | `134217728` (128 MB) | Max buffered stream size before pass-through |
| `CCSWITCH_RETRY_STREAM_KEEPALIVE_SECONDS` | `15` | SSE keepalive comment interval while buffering |
| `CCSWITCH_RETRY_LOG` | `codex-422-retry.log` next to the script | Log file path |

## Logs

Logs record each request, every retry with its reason and attempt number, locally handled `count_tokens` calls, and proxy start/stop events.

## Limitations of verification

This was built and exercised against a single Windows setup with CC Switch v4.0.6 and a Codex/Responses provider. The unit-level checks cover stream error detection and a simulated stream-error-then-success exchange; it is not a general test suite.

## License

MIT. See [LICENSE](LICENSE).
