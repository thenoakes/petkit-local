"""How a feeding schedule reaches Home Assistant: the compact text payload,
the nested-path templates that read feedState, and the republish after a
panel save. All prompted by a live D4 (issue #3)."""
import json

from aiohttp.test_utils import TestClient, TestServer

from petkit_local.devices.base import Device
from petkit_local.devices.ble import BLERegistry
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.ha.categories import get_entities_for_device
from petkit_local.ha.commands import handle_ha_command
from petkit_local.ha.discovery import build_discovery_payload
from petkit_local.ha.publisher import _compact_feed_schedule
from petkit_local.web.hub import EventHub
from petkit_local.web.panel import create_panel_app

THREE_MEALS = {
    "schedule": [{"re": "1,2,3,4,5,6,7", "it": [
        {"id": "n_21600", "t": 21600, "a1": 5, "a2": 0},
        {"id": "n_39600", "t": 39600, "a1": 5, "a2": 0},
        {"id": "n_75600", "t": 75600, "a1": 5, "a2": 0},
    ], "itemJsonString": "[...]"}],
    "v": 2, "deferred": [],
}


def _d4(feed=None):
    d = Device(device_type="d4", petkit_id=1, serial_number="SN")
    if feed is not None:
        d.config["feed_schedule"] = json.loads(json.dumps(feed))
    return d


def test_three_meals_fit_the_text_entity():
    """The stored form of this schedule was 394 characters; HA's limit is 255."""
    text = _compact_feed_schedule(_d4(THREE_MEALS))
    assert len(text) < 255
    assert json.loads(text) == {"schedule": [{"re": "1,2,3,4,5,6,7", "it": [
        {"id": "n_21600", "t": 21600, "a1": 5},
        {"id": "n_39600", "t": 39600, "a1": 5},
        {"id": "n_75600", "t": 75600, "a1": 5},
    ]}]}


def test_a_dual_hopper_keeps_its_second_amount():
    d = Device(device_type="d4sh", petkit_id=1, serial_number="SN")
    d.config["feed_schedule"] = json.loads(json.dumps(THREE_MEALS))
    meal = json.loads(_compact_feed_schedule(d))["schedule"][0]["it"][0]
    assert meal == {"id": "n_21600", "t": 21600, "a1": 5, "a2": 0}


def test_writing_the_text_back_keeps_the_stored_one_off_feeds():
    stored = dict(THREE_MEALS, deferred=[{"id": "d_20261019_43200", "a1": 5, "a2": 0,
                                          "fire_at": 1792400000}])
    d = _d4(stored)
    entity = next(e for e in get_entities_for_device(d) if e.key == "feeding_schedule")
    handle_ha_command(d, entity, _compact_feed_schedule(d))
    assert d.config["feed_schedule"]["deferred"][0]["id"] == "d_20261019_43200"
    assert len(d.config["feed_schedule"]["schedule"][0]["it"]) == 3


def test_a_three_level_path_defaults_its_middle_level():
    d = _d4()
    entity = next(e for e in get_entities_for_device(d) if e.key == "times_dispensed")
    tmpl = build_discovery_payload(entity, 1, "d4", "D4", "SN",
                                   "petkit-local/1/state")["value_template"]
    assert tmpl == "{{ (value_json.state.feedState | default({})).times | default('') }}"


class _Publisher:
    def __init__(self):
        self.states = []

    async def publish_state(self, device):
        self.states.append(device.petkit_id)


async def test_a_panel_save_republishes_the_state():
    reg = DeviceRegistry()
    reg.get_or_create(petkit_id=1, device_type="d4", serial_number="SN")
    publisher = _Publisher()
    app = create_panel_app(reg, BLERegistry(), EventHub(),
                           {"api_url": "http://x/6/", "capture": False, "capture_dir": "/nope"},
                           None, ha_publisher=publisher)
    async with TestClient(TestServer(app)) as c:
        r = await c.post("/api/devices/1/schedule", json={
            "target": "feed_schedule",
            "value": {"schedule": [{"re": "1,2,3,4,5,6,7", "it": [{"t": 21600, "a1": 5}]}]}})
        assert r.status == 200
    assert publisher.states == [1]
