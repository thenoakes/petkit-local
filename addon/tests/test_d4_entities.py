"""The D4's own state spellings, its entity set, and feeding events over HTTP.

All from a live D4 (fw 1.255, issue #3): `food` 0 with an empty hopper and 1
once filled, `batV` 444 with no batteries in, and HTTP event codes 3/4 for a
feed that HA's event entity rejected until they were named.
"""
import pytest

from petkit_local.devices.base import Device
from petkit_local.devices.state_parsers import parse_state_report
from petkit_local.events.ingest import ha_event_name
from petkit_local.ha.categories import get_entities_for_device
from petkit_local.ha.discovery import build_discovery_payload


@pytest.mark.parametrize("food, low", [(0, 1), (2, 1), (1, 0)])
def test_d4_food_low_reads_empty_and_low_as_low(food, low):
    assert parse_state_report("d4", {"food": food})["foodLow"] == low


def test_d4_food_unknown_leaves_food_low_unset():
    assert "foodLow" not in parse_state_report("d4", {"food": -1})


@pytest.mark.parametrize("bat_v, installed", [(444, 0), (7000, 1)])
def test_d4_battery_installed_from_batv(bat_v, installed):
    assert parse_state_report("d4", {"batV": bat_v})["batteryInstalled"] == installed


def test_other_feeders_do_not_get_the_d4_fields():
    state = parse_state_report("d3", {"food": 0, "batV": 7000})
    assert "foodLow" not in state and "batteryInstalled" not in state


def test_d4_publishes_its_own_food_low_and_no_unfillable_entities():
    entities = {e.key: e for e in get_entities_for_device(Device(device_type="d4", petkit_id=1))}
    assert entities["food_low"].value_path == "state.foodLow"
    assert entities["battery_installed"].value_path == "state.batteryInstalled"
    for gone in ("food_in_bowl", "food_bowl_pct", "eating", "surplus_level",
                 "amount_eaten", "desiccant_days", "device_status"):
        assert gone not in entities, gone
    # Totalled locally from each feed_over's real_amount, so they stay.
    assert "times_dispensed" in entities and "total_dispensed" in entities
    # A D3 keeps the family's.
    d3 = {e.key: e for e in get_entities_for_device(Device(device_type="d3", petkit_id=1))}
    assert d3["food_low"].value_path == "state.food"


@pytest.mark.parametrize("code, name", [("3", "feed_start"), ("4", "feed_over"), ("2", "feed_over")])
def test_feeder_http_codes_fire_their_mqtt_names(code, name):
    assert ha_event_name(code, "d4") == name


def test_an_unnamed_code_passes_through():
    assert ha_event_name("20", "t5") == "20"


def test_event_entities_carry_no_value_template():
    """HA's MQTT event platform needs the payload as a JSON object."""
    device = Device(device_type="d4", petkit_id=1, serial_number="SN")
    event = next(e for e in get_entities_for_device(device) if e.component == "event")
    payload = build_discovery_payload(event, 1, "d4", "PetKit D4 SN", "SN",
                                      "petkit-local/1/state")
    assert "value_template" not in payload
    assert "feed_over" in payload["event_types"]


def test_a_d4_feed_report_decodes_despite_quoting_and_a_bare_id():
    """A D4 wraps content/state in literal quotes and leaves the meal id
    unquoted; both used to make the whole content decode as empty."""
    from petkit_local.events.ingest import parse_event_report_form
    from petkit_local.events.normalize import _as_dict
    body = ('event_type=4&event_id=414647898_1792256702&timestamp=1792256707'
            '&content="{"start_time":1792256702,"id":s_20261017_61500,'
            '"day":20261017,"manual":0,"time":61506,"real_amount":10,'
            '"online_state":1,"completed_at":1792256707,"result":0,"err_code":0}"'
            '&state="{"err":{"DC":0},"DCV":5101,"feeding":0}"')
    form = parse_event_report_form(body)
    content = _as_dict(form["content"])
    assert content["real_amount"] == 10 and content["id"] == "s_20261017_61500"
    assert content["result"] == 0
    assert _as_dict(form["state"]) == {"err": {"DC": 0}, "DCV": 5101, "feeding": 0}


def test_an_ordinary_form_value_is_left_alone():
    from petkit_local.events.ingest import parse_event_report_form
    form = parse_event_report_form('event_type=3&content={"id":"x"}&note="hi"')
    assert form["content"] == {"id": "x"} and form["note"] == '"hi"'


def test_a_feeder_result_is_decoded_as_a_feed_outcome():
    """`result` 4 on a D4 is "nothing dispensed", not the litter table's
    "stopped (bin full)"; a litter box keeps its own vocabulary."""
    from petkit_local.events.decode import decode_content
    content = {"result": 4, "real_amount": 0}
    feeder = {f.key: f.text for f in decode_content("4", content, "d4")}
    assert feeder["result"] == "nothing dispensed (zero portions, or hopper empty)"
    litter = {f.key: f.text for f in decode_content("5", {"result": 4}, "t4")}
    assert litter["result"] != feeder["result"]


def test_only_a_d4_edits_its_meals_in_grams():
    from petkit_local.devices import defaults
    for device_type, expected in (("d4", True), ("d4sh", False), ("d4h", False)):
        device = Device(device_type=device_type, petkit_id=1, serial_number="SN")
        target = next(t for t in defaults.schedule_targets(device) if t["kind"] == "feed")
        assert target["grams"] is expected, device_type
