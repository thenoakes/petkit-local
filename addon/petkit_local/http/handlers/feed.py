"""dev_feed_get — the feeder's scheduled meals.

A feeder dispenses on its own clock, from the schedule it fetches here; the
server is not in the loop at feeding time. So the answer must always be a
well-formed schedule — an unidentified device gets an empty-but-valid one rather
than an error, which leaves it dispensing nothing instead of retrying forever.

Everything below matches 21 real cloud responses captured from a D4SH on
2026-08-12 (proxy capture, `proxy_http.jsonl`):

* Meal times — ``t`` in ``schedule[].it[]`` — are SECONDS since local midnight
  (``n_46560`` fires at 12:56:00), not the minutes every other schedule on
  these devices counts. The ``n_`` id suffix is that same seconds value.
* ``latest[]`` lists the concrete feeds firing TODAY OR TOMORROW (local days),
  nothing further out: a Sunday meal polled on Wednesday is in ``schedule``
  but never in ``latest``. Each entry's ``t`` is a live countdown — seconds
  from now, floored, recomputed on every poll. Ids: ``s_YYYYMMDD_SSSSS`` is a
  concrete instance of a recurring meal (suffix = the meal's ``t``),
  ``d_YYYYMMDD_SSSSS`` a one-off deferred feed.
* ``nextTick`` is the countdown of the LAST ``latest`` entry — re-poll when
  the final listed feed has fired — and the constant 86340 when ``latest`` is
  empty (all five empty-schedule captures), i.e. try again in about a day.

A single-amount feeder (`const.DEVICE_TYPES_FEEDER_SINGLE_AMOUNT`, the D4) gets
the same shape with each meal's amount keyed ``a`` instead of ``a1``/``a2`` —
see `wire_amounts`. What is stored stays ``a1``/``a2`` for every model.

A D4 also runs its meals against UTC time of day
(`const.DEVICE_TYPES_FEEDER_UTC_SCHEDULE`), so its meal times — and, where that
crosses midnight, their weekdays — are converted to UTC on the wire. What is
stored stays local.
"""
from __future__ import annotations

import json
import math
import time

from aiohttp import web

from petkit_local.http.handlers._common import request_device
from petkit_local.utils.const import (
    DEVICE_TYPES_FEEDER_SINGLE_AMOUNT, DEVICE_TYPES_FEEDER_UTC_SCHEDULE,
)

#: What the cloud returns for ``nextTick`` when ``latest`` is empty — the
#: constant 86340 (23h59m) in every such capture, never a computed value.
_EMPTY_TICK = 86340
_EMPTY_GROUP = {"re": "1,2,3,4,5,6,7", "it": [], "itemJsonString": "[]"}

#: ``itemJsonString`` serialization matching the cloud byte-for-byte: keys in
#: alphabetical order (``a1``, ``a2``, ``id``, ``t``), no whitespace.
_ITEM_JSON = dict(separators=(",", ":"), sort_keys=True)


def _local_midnight(now: float, day_offset: int) -> float:
    """Local midnight ``day_offset`` days after the day containing ``now``.

    Re-localized through ``mktime`` rather than ``+ 86400`` — DST makes days
    23 and 25 hours long.
    """
    lt = time.localtime(now)
    return time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday + day_offset,
                        0, 0, 0, 0, 0, -1))


def migrate_minute_schedule(feed: dict) -> bool:
    """One-time repair of a schedule saved by the 2.0.0/2.0.1 panel.

    That editor stored meal ``t`` in MINUTES since midnight; the wire unit is
    SECONDS (confirmed against the cloud 2026-08-12), so a stored 1082 had the
    device feeding at 00:18 instead of 18:02. Every value the old editor could
    produce is below 1440, so a schedule whose meals ALL are — and that no
    current save has stamped ``v: 2`` — is minute data: convert, and rewrite
    each id to the cloud's ``n_<seconds>`` scheme. A post-fix schedule whose
    meals genuinely all fall before 00:24 carries the stamp and is left alone.
    """
    if feed.get("v") == 2:
        return False
    meals = [it for g in feed.get("schedule") or [] if isinstance(g, dict)
             for it in g.get("it") or [] if isinstance(it, dict)]
    feed["v"] = 2
    if not meals or any(not isinstance(it.get("t"), int)
                        or not 0 <= it["t"] < 1440 for it in meals):
        return False
    for it in meals:
        it["t"] *= 60
        it["id"] = f"n_{it['t']}"
    for g in feed.get("schedule") or []:
        if isinstance(g, dict) and "it" in g:
            g["itemJsonString"] = json.dumps(g["it"], **_ITEM_JSON)
    return True


def _build_latest(feed: dict, now: float) -> list[dict]:
    """Compute ``latest[]``: every feed firing today or tomorrow, local days.

    Both kinds land here — ``s_`` instances of recurring meals and ``d_``
    deferred feeds — sorted by countdown. ``t`` is ``floor(fire - now)``,
    which is why the cloud's countdowns keep landing on :59.

    The two-day window is what 21 captures show: meals four days out never
    appeared, a deferred feed 27 hours out (tomorrow evening) always did. A
    recurring meal can therefore appear twice, once per day, when both days
    match its weekdays — no capture contradicts it and the device re-polls
    after the last entry anyway (``nextTick``).

    Expired deferred feeds are pruned from ``feed`` as a side effect.
    """
    result = []
    cutoff = _local_midnight(now, 2)  # end of tomorrow

    for day_offset in (0, 1):
        day_start = _local_midnight(now, day_offset)
        # PetKit weekday: Sunday=1 .. Saturday=7; tm_wday: Monday=0.
        pk_wd = (time.localtime(day_start + 43200).tm_wday + 2) % 7 or 7
        date_str = time.strftime("%Y%m%d", time.localtime(day_start + 43200))
        for group in feed.get("schedule") or []:
            days = str(group.get("re", "")).split(",")
            if str(pk_wd) not in (d.strip() for d in days):
                continue
            for item in group.get("it") or []:
                t_secs = item.get("t", 0)
                fire = day_start + t_secs
                if not now < fire < cutoff:
                    continue
                result.append({
                    "id": f"s_{date_str}_{t_secs}",
                    "t": int(fire - now),
                    "a1": item.get("a1", 0),
                    "a2": item.get("a2", 0),
                })

    deferred = feed.get("deferred") or []
    remaining = []
    for d in deferred:
        fire_at = d.get("fire_at", 0)
        if fire_at <= now:
            continue
        remaining.append(d)
        if fire_at >= cutoff:
            continue  # kept for later, but not today-or-tomorrow yet
        result.append({
            "id": d.get("id", ""),
            "t": int(fire_at - now),
            "a1": d.get("a1", 0),
            "a2": d.get("a2", 0),
        })
    if len(remaining) != len(deferred):
        feed["deferred"] = remaining

    result.sort(key=lambda x: x["t"])
    return result


def _compute_next_tick(latest: list[dict]) -> int:
    """The cloud's rule, exact in all 21 captures: the LAST countdown, or the
    86340 constant when nothing is coming up."""
    if not latest:
        return _EMPTY_TICK
    return max(entry["t"] for entry in latest)


def wire_amounts(entries: list[dict], device_type: str) -> list[dict]:
    """Meals or ``latest`` entries keyed the way ``device_type`` reads them.

    A D4SH reads ``a1``/``a2``; a single-amount feeder reads ``a`` alone and
    never looks at the other two, so it is handed ``a1`` as ``a`` and nothing
    else. Returns new dicts: the stored schedule is never rewritten.
    """
    if device_type.lower() not in DEVICE_TYPES_FEEDER_SINGLE_AMOUNT:
        return entries
    out = []
    for entry in entries:
        rekeyed = {k: v for k, v in entry.items() if k not in ("a1", "a2")}
        rekeyed["a"] = entry.get("a1", 0)
        out.append(rekeyed)
    return out


_DAY = 24 * 60 * 60


def _shift_days(re: str, shift: int) -> str:
    """A PetKit weekday mask (Sunday=1 .. Saturday=7) moved ``shift`` days,
    wrapping Saturday into Sunday and back."""
    days = sorted({(int(p) - 1 + shift) % 7 + 1
                   for p in str(re).split(",") if p.strip().isdigit()
                   and 1 <= int(p) <= 7})
    return ",".join(str(d) for d in days)


def _to_utc(groups: list[dict], utc_offset: int) -> list[dict]:
    """Meal times moved from local to UTC seconds of day.

    A meal that crosses midnight in the move lands on the neighbouring
    weekday, so a group whose meals fall either side of it is split into one
    group per weekday shift. Ids follow ``t`` (``n_<seconds>``), as the cloud's
    do.
    """
    out = []
    for group in groups:
        meals = group.get("it") or []
        if not meals:
            out.append(dict(group))
            continue
        by_shift: dict[int, list[dict]] = {}
        for meal in meals:
            t = meal.get("t", 0) - utc_offset
            shift = -1 if t < 0 else 1 if t >= _DAY else 0
            t %= _DAY
            by_shift.setdefault(shift, []).append({**meal, "id": f"n_{t}", "t": t})
        for shift, shifted in sorted(by_shift.items()):
            out.append({**group, "re": _shift_days(group.get("re", ""), shift),
                        "it": shifted})
    return out


def wire_groups(groups: list[dict], device_type: str, *,
                item_json: bool = True, utc_offset: int | None = None) -> list[dict]:
    """Schedule groups for the wire: meals through `wire_amounts`, and the
    ``itemJsonString`` rebuilt from them (``item_json=False`` leaves it out,
    as the property.set push does).

    For a feeder that runs meals on UTC, times are converted first, using
    ``utc_offset`` seconds east of UTC — by default this host's own offset
    right now, the same local time every schedule here is entered in. A DST
    change is therefore picked up at the device's next fetch, which `nextTick`
    keeps within a day.
    """
    if device_type.lower() in DEVICE_TYPES_FEEDER_UTC_SCHEDULE:
        if utc_offset is None:
            utc_offset = time.localtime().tm_gmtoff
        groups = _to_utc(groups, utc_offset)
    out = []
    for group in groups:
        g = {"re": group.get("re", ""),
             "it": wire_amounts(group.get("it") or [], device_type)}
        if item_json:
            g["itemJsonString"] = json.dumps(g["it"], **_ITEM_JSON)
        out.append(g)
    return out


def wire_latest(latest: list[dict], device_type: str, now: float | None = None) -> list[dict]:
    """``latest[]`` for the wire: amounts through `wire_amounts`, and for a
    feeder that runs meals on UTC, each id rebuilt as ``s_<UTC date>_<UTC
    seconds>`` of its fire time, so it names the same instant as the meal's
    converted ``t``. The countdown ``t`` is relative and needs no conversion.

    The id is not decoration: the one D4 meal seen to fire was reported back
    under its ``latest`` id, and that id's numbers matched the meal's ``t``.
    """
    entries = wire_amounts(latest, device_type)
    if device_type.lower() not in DEVICE_TYPES_FEEDER_UTC_SCHEDULE:
        return entries
    if now is None:
        now = time.time()
    out = []
    for entry in entries:
        if not str(entry.get("id", "")).startswith("s_"):
            out.append(entry)
            continue
        # The countdown is floored (see `_build_latest`), so now + t lands up
        # to a second early; ceil puts it back on the meal's own second.
        fire = time.gmtime(math.ceil(now + entry.get("t", 0)))
        secs = fire.tm_hour * 3600 + fire.tm_min * 60 + fire.tm_sec
        out.append({**entry, "id": f"s_{time.strftime('%Y%m%d', fire)}_{secs}"})
    return out


async def handle_feed_get(request: web.Request) -> web.Response:
    """Return the device's stored feeding schedule with live countdowns.

    Returns:
        ``{"result": {"schedule": [...], "nextTick": N, "latest": [...]}}``.
        Structure matches the real cloud's response 1:1.
    """
    device = request_device(request)

    if not device or not device.config.get("feed_schedule"):
        return web.json_response({
            "result": {
                "schedule": [_EMPTY_GROUP],
                "nextTick": _EMPTY_TICK,
                "latest": [],
            }
        })

    feed = device.config["feed_schedule"]
    if not isinstance(feed, dict):
        return web.json_response({"result": feed})

    if migrate_minute_schedule(feed):
        request.app["registry"].save()

    now = time.time()
    latest = _build_latest(feed, now)
    next_tick = _compute_next_tick(latest)

    for group in feed.get("schedule", []):
        if "it" in group:
            group["itemJsonString"] = json.dumps(group["it"], **_ITEM_JSON)

    if device.device_type.lower() in DEVICE_TYPES_FEEDER_SINGLE_AMOUNT:
        return web.json_response({
            "result": {
                "schedule": wire_groups(feed.get("schedule") or [], device.device_type)
                or [_EMPTY_GROUP],
                "nextTick": next_tick,
                "latest": wire_latest(latest, device.device_type, now),
            }
        })

    return web.json_response({
        "result": {
            "schedule": feed.get("schedule", [_EMPTY_GROUP]),
            "nextTick": next_tick,
            "latest": latest,
        }
    })
