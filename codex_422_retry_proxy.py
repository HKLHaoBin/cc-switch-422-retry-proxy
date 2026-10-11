"""Local compatibility and retry wrapper for Claude Desktop requests.

The Claude client sends ``messages/count_tokens`` probes before and during
conversation compaction. CC Switch does not expose that Anthropic endpoint,
so this wrapper answers it locally with a conservative estimate instead of
forwarding it into a 404/connection-storm.

Generation requests keep retrying transient upstream 422s and transient
connection/502 failures until the request succeeds or the proxy is stopped.
"""

from __future__ import annotations

import http.client
import json
import logging
import os
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit


LISTEN_HOST = os.environ.get("CCSWITCH_RETRY_HOST", "127.0.0.1")
LISTEN_PORT = int(os.environ.get("CCSWITCH_RETRY_PORT", "15722"))
TARGET_BASE_URL = os.environ.get(
    "CCSWITCH_RETRY_TARGET",
    "http://127.0.0.1:15721/claude-desktop",
)
TARGET_TIMEOUT_SECONDS = float(os.environ.get("CCSWITCH_RETRY_TARGET_TIMEOUT", "130"))
TARGET_MAX_CONCURRENCY = max(
    1, int(os.environ.get("CCSWITCH_RETRY_TARGET_MAX_CONCURRENCY", "16"))
)
RETRY_BACKOFF_SECONDS = (2, 4, 8, 15, 30)
MAX_REQUEST_BODY_BYTES = 64 * 1024 * 1024
MAX_ERROR_BODY_BYTES = 1024 * 1024
MAX_BUFFERED_STREAM_BYTES = int(
    os.environ.get("CCSWITCH_RETRY_MAX_BUFFERED_STREAM_BYTES", str(128 * 1024 * 1024))
)
STREAM_KEEPALIVE_SECONDS = float(
    os.environ.get("CCSWITCH_RETRY_STREAM_KEEPALIVE_SECONDS", "15")
)
RETRYABLE_PROXY_STATUSES = {502, 503, 504}

TRANSIENT_422_MARKERS = (
    "service_unavailable",
    "temporarily unavailable",
    "servers are currently overloaded",
    "server is currently overloaded",
    "upstream overloaded",
)

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}

LOG_PATH = Path(
    os.environ.get(
        "CCSWITCH_RETRY_LOG",
        str(Path(__file__).resolve().parent / "codex-422-retry.log"),
    )
)
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("ccswitch-422-retry")


def _is_count_tokens_request(request_path: str) -> bool:
    return urlsplit(request_path).path.rstrip("/").endswith("/messages/count_tokens")


def _estimate_input_tokens(body: bytes) -> int:
    """Return a conservative local estimate for Anthropic count_tokens.

    The exact Codex tokenizer is not available at this compatibility layer.
    Three UTF-8 bytes per token intentionally errs on the high side so Claude
    compacts a little earlier instead of underestimating the context.
    """

    try:
        payload = json.loads(body)
        normalized = json.dumps(
            payload, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeDecodeError):
        normalized = body
    return max(1, (len(normalized) + 2) // 3)


def _target_path(request_path: str) -> tuple[str, int, str]:
    target = urlsplit(TARGET_BASE_URL)
    base_path = target.path.rstrip("/")
    path = request_path if request_path.startswith("/") else f"/{request_path}"
    if path == base_path:
        suffix = ""
    elif path.startswith(f"{base_path}/"):
        suffix = path[len(base_path) :]
    else:
        suffix = path
    return target.hostname or "127.0.0.1", target.port or 80, f"{base_path}{suffix}"


def _is_transient_422(body: bytes) -> bool:
    text = body.decode("utf-8", errors="replace").lower()
    return any(marker in text for marker in TRANSIENT_422_MARKERS)


def _is_stream_request(body: bytes) -> bool:
    try:
        payload = json.loads(body)
    except (TypeError, ValueError):
        return False
    return isinstance(payload, dict) and payload.get("stream") is True


def _stream_error_reason(body: bytes) -> str | None:
    text = body.decode("utf-8", errors="replace")
    lowered = text.lower()
    if "error decoding response body" in lowered:
        return "error decoding response body"

    for block in text.split("\n\n"):
        lines = [line.strip() for line in block.splitlines()]
        if any(line.lower() == "event: error" for line in lines):
            return "SSE error event"
        for line in lines:
            if not line.lower().startswith("data:"):
                continue
            raw = line[5:].strip()
            if raw == "[DONE]":
                continue
            try:
                payload = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if not isinstance(payload, dict):
                continue
            if payload.get("type") in {"error", "response.failed"}:
                return f"SSE {payload.get('type')} event"
    return None


def _stream_has_terminal_event(body: bytes) -> bool:
    text = body.decode("utf-8", errors="replace")
    for block in text.split("\n\n"):
        lines = [line.strip() for line in block.splitlines()]
        if any(
            line.lower() in {"event: message_stop", "event: response.completed"}
            for line in lines
        ):
            return True
        for line in lines:
            if not line.lower().startswith("data:"):
                continue
            raw = line[5:].strip()
            if raw == "[DONE]":
                return True
            try:
                payload = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if isinstance(payload, dict) and payload.get("type") in {
                "message_stop",
                "response.completed",
                "response.done",
            }:
                return True
    return False


def _buffer_stream_response(
    handler: BaseHTTPRequestHandler,
    response: http.client.HTTPResponse,
) -> tuple[bytes, bool, str, bool]:
    """Buffer one SSE attempt until it is valid, failed, or too large.

    The fourth return value indicates that the size limit was reached. In that
    case the already buffered prefix is flushed and the proxy falls back to
    ordinary streaming because replaying an unbounded response is unsafe.
    """

    chunks: list[bytes] = []
    total = 0
    last_keepalive = time.monotonic()
    try:
        while True:
            chunk = response.read(64 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > MAX_BUFFERED_STREAM_BYTES:
                return (
                    b"".join(chunks),
                    False,
                    "stream buffer limit exceeded",
                    True,
                )
            now = time.monotonic()
            if now - last_keepalive >= STREAM_KEEPALIVE_SECONDS:
                if not _write_stream_comment(
                    handler, "retry proxy is validating the upstream stream"
                ):
                    return b"", False, "client disconnected", False
                last_keepalive = now
    except Exception as exc:
        return b"".join(chunks), False, f"stream read failed: {exc}", False

    body = b"".join(chunks)
    error_reason = _stream_error_reason(body)
    if error_reason:
        return body, False, error_reason, False
    if not _stream_has_terminal_event(body):
        return body, False, "stream ended before a terminal event", False
    return body, True, "", False


def _write_stream_bytes(handler: BaseHTTPRequestHandler, data: bytes) -> bool:
    try:
        handler.wfile.write(data)
        handler.wfile.flush()
        return True
    except (BrokenPipeError, ConnectionResetError):
        logger.info("client disconnected while waiting for upstream")
        return False


def _start_stream_response(handler: BaseHTTPRequestHandler) -> bool:
    handler.send_response(200, "OK")
    handler.send_header("Content-Type", "text/event-stream")
    handler.send_header("Cache-Control", "no-cache")
    handler.send_header("X-Accel-Buffering", "no")
    handler.send_header("Connection", "close")
    handler.end_headers()
    handler.close_connection = True
    return _write_stream_bytes(handler, b": retry-proxy connected\n\n")


def _write_stream_comment(handler: BaseHTTPRequestHandler, message: str) -> bool:
    safe_message = message.replace("\r", " ").replace("\n", " ")
    return _write_stream_bytes(handler, f": {safe_message}\n\n".encode("utf-8"))


def _write_stream_error(handler: BaseHTTPRequestHandler, body: bytes) -> bool:
    message = body.decode("utf-8", errors="replace").replace("\r", " ")
    message = message.replace("\n", " ")[:2000]
    payload = {
        "type": "error",
        "error": {"type": "api_error", "message": message},
    }
    event = (
        "event: error\n"
        f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
        'event: message_stop\ndata: {"type":"message_stop"}\n\n'
    )
    return _write_stream_bytes(handler, event.encode("utf-8"))


def _copy_response_body(
    handler: BaseHTTPRequestHandler,
    response: http.client.HTTPResponse,
    connection: http.client.HTTPConnection,
) -> None:
    try:
        while True:
            chunk = response.read(64 * 1024)
            if not chunk:
                break
            if not _write_stream_bytes(handler, chunk):
                break
    finally:
        connection.close()


def _copy_response(
    handler: BaseHTTPRequestHandler,
    response: http.client.HTTPResponse,
    connection: http.client.HTTPConnection,
) -> None:
    handler.send_response(response.status, response.reason)
    for name, value in response.getheaders():
        lower_name = name.lower()
        if lower_name in HOP_BY_HOP_HEADERS or lower_name == "content-length":
            continue
        try:
            handler.send_header(name, value)
        except (BrokenPipeError, ConnectionResetError):
            connection.close()
            return
    handler.send_header("Connection", "close")
    handler.end_headers()
    handler.close_connection = True
    _copy_response_body(handler, response, connection)


def _send_buffered(
    handler: BaseHTTPRequestHandler,
    status: int,
    reason: str | None,
    headers: list[tuple[str, str]],
    body: bytes,
) -> None:
    handler.send_response(status, reason)
    for name, value in headers:
        lower_name = name.lower()
        if lower_name in HOP_BY_HOP_HEADERS or lower_name in {"content-length", "connection"}:
            continue
        handler.send_header(name, value)
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Connection", "close")
    handler.end_headers()
    handler.close_connection = True
    try:
        handler.wfile.write(body)
        handler.wfile.flush()
    except (BrokenPipeError, ConnectionResetError):
        logger.info("client disconnected while returning buffered response")


def _send_local_count_tokens(
    handler: BaseHTTPRequestHandler, body: bytes
) -> None:
    estimated_tokens = _estimate_input_tokens(body)
    response_body = json.dumps(
        {"input_tokens": estimated_tokens},
        separators=(",", ":"),
    ).encode("utf-8")
    logger.info(
        "handled count_tokens locally body_bytes=%d estimated_input_tokens=%d",
        len(body),
        estimated_tokens,
    )
    _send_buffered(
        handler,
        200,
        "OK",
        [
            ("Content-Type", "application/json; charset=utf-8"),
            ("X-CCSwitch-Retry-Proxy", "local-count-tokens"),
        ],
        response_body,
    )


class RetryProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: object) -> None:
        logger.info("%s - %s", self.address_string(), fmt % args)

    def do_GET(self) -> None:
        self._proxy_request(retry_422=False)

    def do_HEAD(self) -> None:
        self._proxy_request(retry_422=False)

    def do_OPTIONS(self) -> None:
        self._proxy_request(retry_422=False)

    def do_POST(self) -> None:
        self._proxy_request(retry_422=True)

    def do_PUT(self) -> None:
        self._proxy_request(retry_422=True)

    def do_DELETE(self) -> None:
        self._proxy_request(retry_422=True)

    def _read_body(self) -> bytes:
        raw_length = self.headers.get("Content-Length")
        transfer_encoding = self.headers.get("Transfer-Encoding", "").lower()
        if "chunked" in transfer_encoding:
            chunks: list[bytes] = []
            total = 0
            while True:
                size_line = self.rfile.readline(64 * 1024)
                if not size_line:
                    raise ValueError("incomplete chunked request body")
                size_text = size_line.split(b";", 1)[0].strip()
                try:
                    size = int(size_text, 16)
                except ValueError as exc:
                    raise ValueError("invalid chunked request body") from exc
                if size == 0:
                    while True:
                        trailer = self.rfile.readline(64 * 1024)
                        if not trailer or trailer in {b"\r\n", b"\n"}:
                            break
                    break
                total += size
                if total > MAX_REQUEST_BODY_BYTES:
                    raise ValueError("request body is too large")
                chunk = self.rfile.read(size)
                if len(chunk) != size:
                    raise ValueError("incomplete chunked request body")
                if self.rfile.read(2) != b"\r\n":
                    raise ValueError("invalid chunk terminator")
                chunks.append(chunk)
            return b"".join(chunks)
        if not raw_length:
            return b""
        length = int(raw_length)
        if length < 0 or length > MAX_REQUEST_BODY_BYTES:
            raise ValueError("request body is too large")
        return self.rfile.read(length)

    def _request_headers(self, body: bytes) -> dict[str, str]:
        headers: dict[str, str] = {}
        for name, value in self.headers.items():
            if name.lower() in HOP_BY_HOP_HEADERS:
                continue
            if name.lower() in {"host", "content-length", "expect"}:
                continue
            headers[name] = value
        headers["Content-Length"] = str(len(body))
        headers["Connection"] = "close"
        return headers

    def _proxy_request(self, retry_422: bool) -> None:
        try:
            body = self._read_body()
        except (ValueError, OverflowError) as exc:
            _send_buffered(
                self,
                413,
                "Request Entity Too Large",
                [("Content-Type", "application/json")],
                f'{{"error":"{exc}"}}'.encode(),
            )
            return

        logger.info(
            "request path=%s body_bytes=%d transfer_encoding=%s stream=%s",
            self.path,
            len(body),
            self.headers.get("Transfer-Encoding", ""),
            retry_422 and _is_stream_request(body),
        )
        if self.command == "POST" and _is_count_tokens_request(self.path):
            _send_local_count_tokens(self, body)
            return

        host, port, path = _target_path(self.path)
        headers = self._request_headers(body)
        attempt = 0
        stream_started = retry_422 and _is_stream_request(body)
        if stream_started and not _start_stream_response(self):
            return

        self.server.target_slots.acquire()
        try:
            while True:
                connection = http.client.HTTPConnection(
                    host,
                    port,
                    timeout=TARGET_TIMEOUT_SECONDS,
                )
                try:
                    connection.request(
                        self.command,
                        path,
                        body=body if self.command not in {"GET", "HEAD"} else None,
                        headers=headers,
                    )
                    response = connection.getresponse()
                except Exception as exc:
                    connection.close()
                    if not retry_422:
                        logger.warning("target request failed without retry: %s", exc)
                        if stream_started:
                            _write_stream_error(
                                self,
                                b"Retry proxy could not reach CC Switch.",
                            )
                            return
                        _send_buffered(
                            self,
                            502,
                            "Bad Gateway",
                            [("Content-Type", "text/plain; charset=utf-8")],
                            b"Retry proxy could not reach CC Switch.",
                        )
                        return
                    attempt += 1
                    backoff = RETRY_BACKOFF_SECONDS[
                        min(attempt - 1, len(RETRY_BACKOFF_SECONDS) - 1)
                    ]
                    logger.warning(
                        "target request failed; retrying attempt=%d backoff=%ss path=%s error=%s",
                        attempt,
                        backoff,
                        self.path,
                        exc,
                    )
                    if stream_started and not _write_stream_comment(
                        self,
                        f"CC Switch connection failed; retrying attempt {attempt}",
                    ):
                        return
                    if self.server.retry_stop.wait(backoff):
                        return
                    continue

                if retry_422 and response.status == 422:
                    error_body = response.read(MAX_ERROR_BODY_BYTES)
                    stream_error = (
                        stream_started and _stream_error_reason(error_body) is not None
                    )
                    if _is_transient_422(error_body) or stream_error:
                        connection.close()
                        attempt += 1
                        backoff = RETRY_BACKOFF_SECONDS[
                            min(attempt - 1, len(RETRY_BACKOFF_SECONDS) - 1)
                        ]
                        logger.warning(
                            "retryable 422 from CC Switch; retrying attempt=%d "
                            "backoff=%ss path=%s reason=%s",
                            attempt,
                            backoff,
                            self.path,
                            "stream error" if stream_error else "transient upstream",
                        )
                        if stream_started and not _write_stream_comment(
                            self,
                            f"upstream overloaded; retrying attempt {attempt}",
                        ):
                            return
                        if self.server.retry_stop.wait(backoff):
                            return
                        continue
                    headers_out = response.getheaders()
                    status = response.status
                    reason = response.reason
                    connection.close()
                    if stream_started:
                        _write_stream_error(self, error_body)
                        return
                    _send_buffered(self, status, reason, headers_out, error_body)
                    return

                if retry_422 and response.status in RETRYABLE_PROXY_STATUSES:
                    error_body = response.read(MAX_ERROR_BODY_BYTES)
                    connection.close()
                    attempt += 1
                    backoff = RETRY_BACKOFF_SECONDS[
                        min(attempt - 1, len(RETRY_BACKOFF_SECONDS) - 1)
                    ]
                    logger.warning(
                        "transient HTTP %d from CC Switch; retrying attempt=%d backoff=%ss path=%s",
                        response.status,
                        attempt,
                        backoff,
                        self.path,
                    )
                    if stream_started and not _write_stream_comment(
                        self,
                        f"CC Switch returned HTTP {response.status}; retrying attempt {attempt}",
                    ):
                        return
                    if self.server.retry_stop.wait(backoff):
                        return
                    continue

                if response.status >= 400:
                    buffered = response.read(MAX_ERROR_BODY_BYTES)
                    headers_out = response.getheaders()
                    status = response.status
                    reason = response.reason
                    connection.close()
                    if stream_started:
                        _write_stream_error(self, buffered)
                        return
                    _send_buffered(self, status, reason, headers_out, buffered)
                    return

                if stream_started:
                    buffered, valid, reason, overflowed = _buffer_stream_response(
                        self, response
                    )
                    if overflowed:
                        logger.warning(
                            "stream exceeded buffered validation limit=%d bytes; "
                            "switching to pass-through path=%s",
                            MAX_BUFFERED_STREAM_BYTES,
                            self.path,
                        )
                        if buffered and not _write_stream_bytes(self, buffered):
                            connection.close()
                            return
                        _copy_response_body(self, response, connection)
                        return

                    connection.close()
                    if valid:
                        for offset in range(0, len(buffered), 64 * 1024):
                            if not _write_stream_bytes(
                                self, buffered[offset : offset + 64 * 1024]
                            ):
                                return
                        return

                    if reason == "client disconnected":
                        return
                    attempt += 1
                    backoff = RETRY_BACKOFF_SECONDS[
                        min(attempt - 1, len(RETRY_BACKOFF_SECONDS) - 1)
                    ]
                    logger.warning(
                        "invalid upstream stream; retrying attempt=%d backoff=%ss "
                        "path=%s reason=%s",
                        attempt,
                        backoff,
                        self.path,
                        reason,
                    )
                    if not _write_stream_comment(
                        self,
                        f"invalid upstream stream ({reason}); "
                        f"retrying attempt {attempt}",
                    ):
                        return
                    if self.server.retry_stop.wait(backoff):
                        return
                    continue

                _copy_response(self, response, connection)
                return
        finally:
            self.server.target_slots.release()


class RetryProxyServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int]):
        super().__init__(address, RetryProxyHandler)
        self.retry_stop = threading.Event()
        self.target_slots = threading.BoundedSemaphore(TARGET_MAX_CONCURRENCY)


def main() -> None:
    server = RetryProxyServer((LISTEN_HOST, LISTEN_PORT))

    def stop_server(signum: int, _frame: object) -> None:
        logger.info("stopping retry proxy signal=%s", signum)
        server.retry_stop.set()
        server.shutdown()

    signal.signal(signal.SIGTERM, stop_server)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, stop_server)

    logger.info(
        "retry proxy listening on %s:%s target=%s",
        LISTEN_HOST,
        LISTEN_PORT,
        TARGET_BASE_URL,
    )
    try:
        server.serve_forever()
    finally:
        server.retry_stop.set()
        server.server_close()
        logger.info("retry proxy stopped")


if __name__ == "__main__":
    main()
