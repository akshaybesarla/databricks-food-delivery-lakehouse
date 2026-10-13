import json

from fooddelivery.simulator import FoodDeliverySimulator, write_batch


def test_same_seed_gives_same_data():
    a = FoodDeliverySimulator(seed=7).next_batch()
    b = FoodDeliverySimulator(seed=7).next_batch()
    assert a == b


def test_simulation_resumes_from_saved_state():
    straight = FoodDeliverySimulator(seed=1)
    straight.next_batch()
    expected = straight.next_batch()

    first = FoodDeliverySimulator(seed=1)
    first.next_batch()
    resumed = FoodDeliverySimulator.from_state_json(first.state_json(), seed=1)
    assert resumed.next_batch() == expected


def test_first_batch_is_full_restaurant_snapshot_then_only_changes():
    sim = FoodDeliverySimulator()
    first, second = sim.next_batch(), sim.next_batch()
    assert len(first["restaurants"]) == len(sim.state.restaurants)
    assert 0 < len(second["restaurants"]) < len(first["restaurants"])


def test_schema_drift_tip_amount_appears_after_rollout():
    sim = FoodDeliverySimulator(tip_rollout_batch=2)
    batches = [sim.next_batch() for _ in range(4)]
    assert not any("tip_amount" in e for b in batches[:2] for e in b["order_events"])
    assert any("tip_amount" in e for e in batches[3]["order_events"])


def test_feed_contains_duplicates_and_bad_records():
    sim = FoodDeliverySimulator(dirty_rate=0.05)
    events = [e for _ in range(3) for e in sim.next_batch()["order_events"]]
    ids = [e["event_id"] for e in events]
    assert len(ids) > len(set(ids)), "expected duplicate event_ids (client retries)"
    assert any(e["order_id"] is None or e["restaurant_id"] is None for e in events)


def test_write_batch_writes_json_lines(tmp_path):
    batch = FoodDeliverySimulator().next_batch()
    counts = write_batch(batch, str(tmp_path))
    lines = (tmp_path / "order_events" / "order_events_00000.json").read_text().splitlines()
    assert counts["order_events"] == len(lines) == len(batch["order_events"])
    json.loads(lines[0])
    assert not list(tmp_path.glob("*/_tmp_*")), "temporary files must be renamed"
