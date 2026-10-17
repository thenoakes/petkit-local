"""dev_feed_get must match the real cloud's responses — 21 of them were
captured through proxy mode from a D4SH on 2026-08-12, and the replay test
below reproduces one to the second."""
import json
import os
import time

import pytest
from aiohttp.test_utils import TestClient, TestServer

from petkit_local.devices.registry import DeviceRegistry
from petkit_local.http.handlers.feed import (
    _build_latest,
    _compute_next_tick,
    migrate_minute_schedule,
    wire_groups,
    wire_latest,
)
from petkit_local.http.server import create_app


@pytest.fixture
def utc_tz():
    """Pins the host to UTC, so a D4's UTC meal times equal the local ones."""
    old = os.environ.get("TZ")
    os.environ["TZ"] = "UTC"
    time.tzset()
    yield
    if old is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = old
    time.tzset()


@pytest.fixture
def warsaw_tz():
    """The capture came from a device in Europe/Warsaw; countdown values
    depend on where local midnight is."""
    old = os.environ.get("TZ")
    os.environ["TZ"] = "Europe/Warsaw"
    time.tzset()
    yield
    if old is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = old
    time.tzset()


def _app(device_type="d4sh"):
    reg = DeviceRegistry()
    reg.get_or_create(petkit_id=1, device_type=device_type, serial_number="SN1")
    config = {"api_url": "http://localhost:8080", "bucket_endpoint": "https://localhost:9000"}
    app = create_app(reg, config)
    return app, reg


async def _get_feed(client):
    r = await client.get("/6/d4sh/dev_feed_get",
                         headers={"X-Device": "id=1&nonce=x&timestamp=1&type=d4sh&sign=x"})
    return (await r.json())["result"]


# --- replay of a real cloud response ----------------------------------------
# proxy_http.jsonl, the longest dev_feed_get line: Wed 2026-08-12 18:01:38
# local. Four groups — Thursday's n_46560, Sunday's n_50100/n_71700,
# Wednesday's n_60540 (16:49, already past at capture time) — plus a deferred
# feed for Thursday 17:05. The cloud answered with exactly two latest entries:
# only tomorrow's feeds, Sunday's and next Wednesday's excluded.
CAPTURE_TS = 1786550498.7870133
CAPTURE_SCHEDULE = [
    {"re": "2,3,6,7", "it": []},
    {"re": "5", "it": [{"id": "n_46560", "t": 46560, "a1": 1, "a2": 0}]},
    {"re": "1", "it": [{"id": "n_50100", "t": 50100, "a1": 1, "a2": 0},
                       {"id": "n_71700", "t": 71700, "a1": 1, "a2": 6}]},
    {"re": "4", "it": [{"id": "n_60540", "t": 60540, "a1": 1, "a2": 0}]},
]
CLOUD_LATEST = [
    {"id": "s_20260813_46560", "t": 68061, "a1": 1, "a2": 0},
    {"id": "d_20260813_61500", "t": 83001, "a1": 0, "a2": 1},
]
CLOUD_NEXT_TICK = 83001


def test_replay_matches_cloud_exactly(warsaw_tz):
    deferred_fire = time.mktime((2026, 8, 13, 17, 5, 0, 0, 0, -1))
    feed = {
        "schedule": [dict(g, it=[dict(i) for i in g["it"]])
                     for g in CAPTURE_SCHEDULE],
        "deferred": [{"id": "d_20260813_61500", "a1": 0, "a2": 1,
                      "fire_at": deferred_fire}],
        "v": 2,
    }
    latest = _build_latest(feed, CAPTURE_TS)
    assert latest == CLOUD_LATEST
    assert _compute_next_tick(latest) == CLOUD_NEXT_TICK


def test_far_future_meals_stay_out_of_latest(warsaw_tz):
    """Sunday's meals, four days from the capture moment, are in `schedule`
    but the cloud never put them in `latest` — the window is today+tomorrow."""
    feed = {"schedule": [
        {"re": "1", "it": [{"id": "n_50100", "t": 50100, "a1": 1, "a2": 0}]},
    ], "v": 2}
    assert _build_latest(feed, CAPTURE_TS) == []
    assert _compute_next_tick([]) == 86340


def test_todays_own_passed_meal_is_excluded(warsaw_tz):
    """n_60540 is 16:49 on a Wednesday; at 18:01 the same Wednesday it is
    past, and the cloud did not list next week's instance either."""
    feed = {"schedule": [
        {"re": "4", "it": [{"id": "n_60540", "t": 60540, "a1": 1, "a2": 0}]},
    ], "v": 2}
    assert _build_latest(feed, CAPTURE_TS) == []


# --- serve-time behaviour ----------------------------------------------------

async def test_empty_schedule_matches_cloud():
    """No schedule: one all-week empty group, latest [], and the cloud's
    86340 constant — never a computed tick."""
    app, reg = _app()
    async with TestClient(TestServer(app)) as c:
        result = await _get_feed(c)
        assert set(result.keys()) == {"schedule", "nextTick", "latest"}
        assert result["schedule"] == [
            {"re": "1,2,3,4,5,6,7", "it": [], "itemJsonString": "[]"}]
        assert result["nextTick"] == 86340
        assert result["latest"] == []


async def test_recurring_meal_appears_with_live_countdown():
    """An all-week meal 2h from now must produce an s_ entry whose t is the
    seconds until it fires, and nextTick must be the last entry's t."""
    app, reg = _app()
    d = reg.get(1)
    now = time.time()
    lt = time.localtime(now)
    secs_since_midnight = lt.tm_hour * 3600 + lt.tm_min * 60 + lt.tm_sec
    future_t = (secs_since_midnight + 7200) % 86400
    d.config["feed_schedule"] = {
        "schedule": [{"re": "1,2,3,4,5,6,7", "it": [
            {"id": f"n_{future_t}", "t": future_t, "a1": 1, "a2": 0},
        ]}],
        "v": 2,
    }
    async with TestClient(TestServer(app)) as c:
        result = await _get_feed(c)
        entry = result["latest"][0]
        assert entry["id"].startswith("s_")
        assert entry["a1"] == 1 and entry["a2"] == 0
        assert 7100 < entry["t"] < 7300
        assert result["nextTick"] == max(e["t"] for e in result["latest"])


async def test_itemJsonString_matches_cloud_byte_for_byte():
    """The cloud serializes it[] with keys in alphabetical order and no
    whitespace."""
    app, reg = _app()
    d = reg.get(1)
    d.config["feed_schedule"] = {
        "schedule": [{"re": "5", "it": [
            {"id": "n_46560", "t": 46560, "a1": 1, "a2": 0},
        ]}],
        "v": 2,
    }
    async with TestClient(TestServer(app)) as c:
        result = await _get_feed(c)
        assert result["schedule"][0]["itemJsonString"] == \
            '[{"a1":1,"a2":0,"id":"n_46560","t":46560}]'
        assert json.loads(result["schedule"][0]["itemJsonString"]) == \
            result["schedule"][0]["it"]


async def test_deferred_feed_appears_in_latest():
    app, reg = _app()
    d = reg.get(1)
    future = time.time() + 3600
    d.config["feed_schedule"] = {
        "schedule": [{"re": "1,2,3,4,5,6,7", "it": []}],
        "deferred": [{"id": "d_20260813_61500", "a1": 0, "a2": 1, "fire_at": future}],
        "v": 2,
    }
    async with TestClient(TestServer(app)) as c:
        result = await _get_feed(c)
        d_entries = [e for e in result["latest"] if e["id"].startswith("d_")]
        assert len(d_entries) == 1
        assert 3550 < d_entries[0]["t"] < 3650
        assert result["nextTick"] == d_entries[0]["t"]


async def test_expired_deferred_is_pruned():
    app, reg = _app()
    d = reg.get(1)
    d.config["feed_schedule"] = {
        "schedule": [{"re": "1,2,3,4,5,6,7", "it": []}],
        "deferred": [{"id": "d_20260812_10000", "a1": 1, "a2": 0, "fire_at": time.time() - 60}],
        "v": 2,
    }
    async with TestClient(TestServer(app)) as c:
        result = await _get_feed(c)
        assert result["latest"] == []
        assert d.config["feed_schedule"]["deferred"] == []


async def test_far_future_deferred_is_kept_but_not_listed():
    """A deferred feed past tomorrow stays stored — it must not be pruned —
    but does not enter latest until its day comes into the window."""
    app, reg = _app()
    d = reg.get(1)
    far = time.time() + 5 * 86400
    d.config["feed_schedule"] = {
        "schedule": [{"re": "1,2,3,4,5,6,7", "it": []}],
        "deferred": [{"id": "d_20260817_50000", "a1": 1, "a2": 0, "fire_at": far}],
        "v": 2,
    }
    async with TestClient(TestServer(app)) as c:
        result = await _get_feed(c)
        assert result["latest"] == []
        assert len(d.config["feed_schedule"]["deferred"]) == 1


# --- minute-schedule migration ----------------------------------------------

def test_minute_schedule_is_migrated():
    """A 2.0.0/2.0.1 panel save stored t in minutes; it must come back as
    seconds with cloud-scheme ids, exactly once."""
    feed = {"schedule": [{"re": "1,2,3,4,5,6,7", "it": [
        {"id": "n_1082", "t": 1082, "a1": 1, "a2": 0},
        {"id": 2, "t": 963, "a1": 1, "a2": 0},
    ]}]}
    assert migrate_minute_schedule(feed) is True
    assert feed["schedule"][0]["it"] == [
        {"id": "n_64920", "t": 64920, "a1": 1, "a2": 0},
        {"id": "n_57780", "t": 57780, "a1": 1, "a2": 0},
    ]
    assert feed["v"] == 2
    assert migrate_minute_schedule(feed) is False


def test_stamped_early_morning_schedule_is_not_migrated():
    """Post-fix, a meal before 00:24 has t < 1440 legitimately; the v stamp
    is what keeps the migration off it."""
    feed = {"schedule": [{"re": "1", "it": [
        {"id": "n_1200", "t": 1200, "a1": 1, "a2": 0},
    ]}], "v": 2}
    assert migrate_minute_schedule(feed) is False
    assert feed["schedule"][0]["it"][0]["t"] == 1200


def test_seconds_schedule_without_stamp_is_left_alone():
    """Any meal at or past 00:24 proves the schedule is seconds-based; only
    the stamp is added."""
    feed = {"schedule": [{"re": "1", "it": [
        {"id": "n_46560", "t": 46560, "a1": 1, "a2": 0},
    ]}]}
    assert migrate_minute_schedule(feed) is False
    assert feed["schedule"][0]["it"][0]["t"] == 46560
    assert feed["v"] == 2


# --- D4: one amount, keyed `a` ----------------------------------------------
# A D4 (fw 1.255) served `a1` fetched every schedule and fired none of its
# meals (issue #3); dwyschka/localkit keys a D4's meals `a`.

async def _get_d4_feed(client):
    r = await client.get("/6/d4/dev_feed_get",
                         headers={"X-Device": "id=1&nonce=x&timestamp=1&type=D4&sign=x"})
    return (await r.json())["result"]


async def test_d4_meals_and_latest_carry_a_not_a1_a2(utc_tz):
    app, reg = _app("d4")
    d = reg.get(1)
    now = time.time()
    lt = time.localtime(now)
    future_t = (lt.tm_hour * 3600 + lt.tm_min * 60 + lt.tm_sec + 7200) % 86400
    d.config["feed_schedule"] = {
        "schedule": [{"re": "1,2,3,4,5,6,7", "it": [
            {"id": f"n_{future_t}", "t": future_t, "a1": 10, "a2": 0},
        ]}],
        "v": 2,
    }
    async with TestClient(TestServer(app)) as c:
        result = await _get_d4_feed(c)
    meal = result["schedule"][0]["it"][0]
    assert meal == {"id": f"n_{future_t}", "t": future_t, "a": 10}
    assert result["schedule"][0]["itemJsonString"] == json.dumps(
        [meal], separators=(",", ":"), sort_keys=True)
    entry = result["latest"][0]
    assert set(entry) == {"id", "t", "a"} and entry["a"] == 10
    assert entry["id"].startswith("s_")
    assert result["nextTick"] == max(e["t"] for e in result["latest"])
    # What is stored is untouched: still a1/a2, for the panel and every model.
    assert d.config["feed_schedule"]["schedule"][0]["it"][0]["a1"] == 10


async def test_d4_with_no_schedule_gets_the_empty_group():
    app, reg = _app("d4")
    async with TestClient(TestServer(app)) as c:
        result = await _get_d4_feed(c)
    assert result["schedule"] == [
        {"re": "1,2,3,4,5,6,7", "it": [], "itemJsonString": "[]"}]
    assert result["nextTick"] == 86340 and result["latest"] == []


async def test_d4sh_keeps_a1_a2():
    app, reg = _app("d4sh")
    reg.get(1).config["feed_schedule"] = {
        "schedule": [{"re": "5", "it": [{"id": "n_46560", "t": 46560, "a1": 1, "a2": 2}]}],
        "v": 2,
    }
    async with TestClient(TestServer(app)) as c:
        result = await _get_feed(c)
    assert result["schedule"][0]["it"][0] == {"id": "n_46560", "t": 46560, "a1": 1, "a2": 2}


# --- D4: meal times in UTC ----------------------------------------------------
# A D4 told `timezone: 1.0` dispensed a meal stored at 16:48 at 17:48 BST, i.e.
# at 16:48 UTC: it runs meals on UTC seconds of day.

def test_d4_meal_times_are_sent_in_utc():
    groups = [{"re": "2,3", "it": [{"id": "n_60480", "t": 60480, "a1": 10, "a2": 0}]}]
    wire = wire_groups(groups, "d4", utc_offset=3600)
    assert wire == [{"re": "2,3", "it": [{"id": "n_56880", "t": 56880, "a": 10}],
                     "itemJsonString": '[{"a":10,"id":"n_56880","t":56880}]'}]
    # What was passed in, i.e. what is stored, stays local.
    assert groups[0]["it"][0]["t"] == 60480


def test_d4_meal_crossing_midnight_moves_to_the_neighbouring_weekday():
    """00:30 local on Sunday (1) at UTC+1 is 23:30 UTC on Saturday (7); a
    later meal in the same group stays on Sunday, so the group is split."""
    groups = [{"re": "1,4", "it": [{"t": 1800, "a1": 5}, {"t": 43200, "a1": 7}]}]
    wire = wire_groups(groups, "d4", utc_offset=3600, item_json=False)
    assert wire == [
        {"re": "3,7", "it": [{"id": "n_84600", "t": 84600, "a": 5}]},
        {"re": "1,4", "it": [{"id": "n_39600", "t": 39600, "a": 7}]},
    ]


def test_d4_meal_crossing_midnight_westward_moves_forward():
    groups = [{"re": "7", "it": [{"t": 84600, "a1": 5}]}]
    wire = wire_groups(groups, "d4", utc_offset=-3600, item_json=False)
    assert wire == [{"re": "1", "it": [{"id": "n_1800", "t": 1800, "a": 5}]}]


def test_d4sh_meal_times_stay_local():
    groups = [{"re": "5", "it": [{"id": "n_46560", "t": 46560, "a1": 1, "a2": 0}]}]
    wire = wire_groups(groups, "d4sh", utc_offset=3600, item_json=False)
    assert wire[0]["it"] == [{"id": "n_46560", "t": 46560, "a1": 1, "a2": 0}]


def test_d4_latest_ids_name_the_fire_time_in_utc():
    """A `latest` id carries the same UTC instant as the meal's converted
    `t`; the countdown itself is relative and unchanged."""
    now = 1792255680.0 - 300  # 5 minutes before 16:48:00 UTC, 2026-10-17
    latest = [{"id": "s_20261017_64080", "t": 300, "a1": 10, "a2": 0}]
    assert wire_latest(latest, "d4", now) == [
        {"id": "s_20261017_60480", "t": 300, "a": 10}]
    assert wire_latest(latest, "d4sh", now) == latest
