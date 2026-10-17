"""The one place a device request is logged, mirrored, captured and counted live.

Outermost of the three, so what it records is what the device actually received
— including whatever `proxy_middleware` substituted. Device liveness is stamped
here for the same reason: *any* HTTP contact proves the device is up, and not
every model does MQTT or even an HTTP heartbeat.

The module is named for what it does; `import logging` anywhere in this package
still reaches the stdlib, because every import in this codebase is absolute and
Python 3 has no implicit relative imports.
"""
from __future__ import annotations

import logging
import time

from aiohttp import web

from petkit_local.http.handlers._common import device_id, request_device
from petkit_local.http.middleware import API_PREFIX, PROXY_OUTCOME, Handler
from petkit_local.utils.capture import capture_record

log = logging.getLogger(__name__)


def _text_or_none(raw: bytes | None) -> str | None:
    """Decode a captured body, never raising on a binary one.

    None passes through as None, which is what `_short` below preserves too:
    a proxy capture has to keep "there was no body" distinct from "the body was
    empty", and those are different facts about what the cloud answered.
    """
    if raw is None:
        return None
    return bytes(raw).decode("utf-8", "replace")


def _short(text: str | None, limit: int = 4000) -> str | None:
    """Cap a body at `limit` chars for the panel log, marking what was cut.

    The live log holds these in memory and ships them to every open browser, so
    an unbounded state_report or file_info body would be paid for repeatedly.
    None passes through as None to keep "no body" distinct from "empty body".
    """
    if text is None:
        return None
    return text if len(text) <= limit else text[:limit] + f"\n... (+{len(text) - limit} bytes truncated)"


@web.middleware
async def logging_middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
    """Log, mirror and capture a device request, and refresh device liveness.

    Only `/6/` paths — the PetKit API prefix — are treated as device traffic;
    the bucket, the face photos and the patcher downloads share this app but are
    not protocol calls and must not mark a device online or flood the panel log.

    For those paths it: emits the one-line access log, records a detailed entry
    (headers, query, capped bodies) on the panel's event hub, appends to the
    capture file when `capture` is configured, and stamps `last_seen` on the
    resolved device — firing the app's ``on_device_seen`` callback on every
    such contact, which re-asserts availability in HA when it has drifted.
    """
    # Pre-read the request body for POST/PUT so we can surface it in the panel
    # log. aiohttp caches the payload, so the handler still reads it normally.
    req_body = None
    if request.method in ("POST", "PUT", "PATCH") and request.path.startswith(API_PREFIX):
        try:
            raw = await request.read()
            if raw:
                req_body = raw.decode("utf-8", "replace")
        except Exception:
            req_body = None

    resp = await handler(request)
    if request.path.startswith(API_PREFIX):
        dt = request.get("device_type", "?")
        pid = request.get("x_device", {}).get("id", "?")
        log.info("%s %s [%s id=%s] -> %d", request.method, request.path, dt, pid, resp.status)
        hub = request.app.get("event_hub")
        if hub is not None:
            did = device_id(request)
            resp_body = None
            try:
                if getattr(resp, "body", None) is not None:
                    resp_body = bytes(resp.body).decode("utf-8", "replace")
            except Exception:
                resp_body = None
            detail = {
                "method": request.method,
                "path": request.path,
                "status": resp.status,
                "device_type": dt,
                "headers": dict(request.headers),
                "query": dict(request.query),
                "req_body": _short(req_body),
                "resp_body": _short(resp_body),
            }
            # What proxy mode did with this request, if anything. Folded into
            # the entry rather than published separately — see `_note_outcome`.
            if PROXY_OUTCOME in request:
                detail["proxy"] = request[PROXY_OUTCOME]
            hub.record_http(did, request.method, request.path, resp.status, detail=detail)

        config = request.app.get("config", {})
        if config.get("capture"):
            capture_record(config.get("capture_dir", "/data/capture"), "requests", {
                "method": request.method,
                "path": request.path,
                "status": resp.status,
                "xdevice": request.headers.get("X-Device", ""),
                "xsession": request.headers.get("X-Session", ""),
                "query": dict(request.query),
            })

        # Any HTTP contact keeps the device online (HTTP is a valid transport,
        # not every device does MQTT / an HTTP heartbeat).
        device = request_device(request)
        if device is not None:
            device.last_seen = time.time()
            # The `type` spelling the firmware itself puts in `X-Device`, kept
            # verbatim. It is one of the five fields hashed into that header's
            # `sign`, so signing a request AS this device needs the device's own
            # spelling and not our lowercase codename — see `http/cloud_fetch.py`.
            # Recorded from live traffic rather than assumed, because a guess
            # here fails as an authentication error and nothing else.
            wire_type = (request.get("x_device") or {}).get("type") or ""
            if wire_type and device.wire_type != wire_type:
                device.wire_type = wire_type
            device.online = True
            cb = request.app.get("on_device_seen")
            if cb is not None:
                await cb(device)
    return resp
