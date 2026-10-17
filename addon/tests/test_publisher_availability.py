"""Availability is re-asserted on contact, so one lost publish cannot leave a
heartbeating device "unavailable" in HA until its next event."""
from petkit_local.devices.registry import DeviceRegistry
from petkit_local.ha.publisher import HAPublisher
from tests._fakes import FakeMqttClient


def _setup():
    reg = DeviceRegistry()
    dev = reg.get_or_create(petkit_id=7, device_type="d4", serial_number="SN")
    pub = HAPublisher(reg, {})
    pub._client = FakeMqttClient()
    pub._connected = True
    return dev, pub


def _availability(pub):
    return [p for t, p, _ in pub._client.published if t == "petkit-local/7/availability"]


async def test_ensure_availability_publishes_once_per_change():
    dev, pub = _setup()
    dev.online = True
    await pub.ensure_availability(dev)
    await pub.ensure_availability(dev)
    assert _availability(pub) == ["online"]
    dev.online = False
    await pub.ensure_availability(dev)
    assert _availability(pub) == ["online", "offline"]


async def test_a_replayed_offline_is_corrected_by_the_next_contact():
    """The restart race: the replay on connect sends "offline", the device is
    already online by the next heartbeat, and that heartbeat must say so."""
    dev, pub = _setup()
    dev.online = False
    await pub.publish_availability(dev)
    dev.online = True
    await pub.ensure_availability(dev)
    assert _availability(pub) == ["offline", "online"]


async def test_nothing_is_recorded_while_disconnected():
    dev, pub = _setup()
    pub._connected = False
    dev.online = True
    await pub.ensure_availability(dev)
    pub._connected = True
    await pub.ensure_availability(dev)
    assert _availability(pub) == ["online"]
