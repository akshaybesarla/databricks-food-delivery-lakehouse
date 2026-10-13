"""Event simulator for a quick-commerce food delivery platform.

Produces the three feeds a real delivery app's backend would publish:

* order_events  - one event per order status change (PLACED -> ACCEPTED ->
                  PICKED_UP -> DELIVERED, or CANCELLED), i.e. a CDC-style feed
* rider_pings   - GPS + status heartbeat from every online delivery partner
* restaurants   - restaurant master data; changes over time (commission,
                  active flag, cuisine) to exercise SCD Type 2

The simulator is deliberately *messy*, because real feeds are:
  - duplicate events (client retries)
  - events that arrive in a later file than their event time (late arrivals)
  - bad records (null ids, negative amounts, unknown statuses, 0,0 GPS)
  - schema drift: app v5.2 starts sending `tip_amount` part-way through

Pure Python (no Spark), deterministic for a given seed, and resumable: the
whole state round-trips through JSON so each job run continues the timeline.
"""

from __future__ import annotations

import json
import math
import os
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from fooddelivery.config import CITIES, CUISINES, PAYMENT_MODES

CITY_CENTRES = {
    "Hyderabad": (17.3850, 78.4867),
    "Bengaluru": (12.9716, 77.5946),
    "Chennai": (13.0827, 80.2707),
    "Mumbai": (19.0760, 72.8777),
}
IST = timedelta(hours=5, minutes=30)
FMT = "%Y-%m-%dT%H:%M:%S.%f"


def _iso(ts: datetime) -> str:
    return ts.strftime(FMT)[:-3] + "Z"


def _parse(s: str) -> datetime:
    return datetime.strptime(s.rstrip("Z") + "000", FMT).replace(tzinfo=timezone.utc)


def demand_multiplier(ts_utc: datetime) -> float:
    """Lunch and dinner peaks in Indian Standard Time."""
    h = (ts_utc + IST).hour + (ts_utc + IST).minute / 60
    lunch = 1.6 * math.exp(-((h - 13.0) ** 2) / 2.0)
    dinner = 2.2 * math.exp(-((h - 20.5) ** 2) / 2.5)
    return max(0.15, 0.35 + lunch + dinner)


@dataclass
class SimState:
    next_batch: int = 0
    clock: str = "2026-10-01T05:30:00.000Z"  # 11:00 IST
    order_seq: int = 0
    pending: list = field(default_factory=list)  # events not yet "arrived"
    rider_trips: dict = field(default_factory=dict)  # rider_id -> [[start, end], ...]
    restaurants: dict = field(default_factory=dict)  # restaurant_id -> record


class FoodDeliverySimulator:
    def __init__(
        self,
        seed: int = 42,
        batch_minutes: int = 15,
        restaurants_per_zone: int = 8,
        riders_per_zone: int = 25,
        base_orders_per_batch: int = 120,
        dirty_rate: float = 0.03,
        tip_rollout_batch: int = 4,
        state: SimState | None = None,
    ):
        self.seed = seed
        self.batch_minutes = batch_minutes
        self.restaurants_per_zone = restaurants_per_zone
        self.riders_per_zone = riders_per_zone
        self.base_orders_per_batch = base_orders_per_batch
        self.dirty_rate = dirty_rate
        self.tip_rollout_batch = tip_rollout_batch
        self.state = state or SimState()
        self.zones = [(c, z) for c, zs in CITIES.items() for z in zs]
        self.riders = {
            f"R{ci:02d}{zi:02d}{n:03d}": (c, z)
            for ci, (c, zs) in enumerate(CITIES.items())
            for zi, z in enumerate(zs)
            for n in range(riders_per_zone)
        }

    # ------------------------------------------------------------------ state
    def state_json(self) -> str:
        return json.dumps(self.state.__dict__)

    @classmethod
    def from_state_json(cls, payload: str, **kwargs) -> "FoodDeliverySimulator":
        return cls(state=SimState(**json.loads(payload)), **kwargs)

    # ---------------------------------------------------------------- helpers
    def _uuid(self, rng: random.Random) -> str:
        return str(uuid.UUID(int=rng.getrandbits(128), version=4))

    def _zone_point(self, rng: random.Random, city: str, zone: str) -> tuple[float, float]:
        lat0, lon0 = CITY_CENTRES[city]
        zi = CITIES[city].index(zone)
        angle = 2 * math.pi * zi / len(CITIES[city])
        lat = lat0 + 0.06 * math.sin(angle) + rng.uniform(-0.015, 0.015)
        lon = lon0 + 0.06 * math.cos(angle) + rng.uniform(-0.015, 0.015)
        return round(lat, 6), round(lon, 6)

    def _restaurant_changes(self, rng: random.Random, now: datetime) -> list[dict]:
        s = self.state
        if not s.restaurants:  # first run: full snapshot
            for ci, (city, zones) in enumerate(CITIES.items()):
                for zi, zone in enumerate(zones):
                    for n in range(self.restaurants_per_zone):
                        rid = f"REST{ci}{zi}{n:03d}"
                        s.restaurants[rid] = {
                            "restaurant_id": rid,
                            "restaurant_name": f"{rng.choice(['Spice', 'Urban', 'Royal', 'Green', 'Tasty', 'Grand'])} "
                            f"{rng.choice(['Kitchen', 'Bowl', 'House', 'Cafe', 'Tiffins', 'Grill'])} {zone}",
                            "city": city,
                            "zone": zone,
                            "cuisine": rng.choice(CUISINES),
                            "commission_pct": rng.choice([15.0, 18.0, 20.0, 22.0, 25.0]),
                            "is_active": True,
                            "updated_at": _iso(now),
                        }
            return [dict(r) for r in s.restaurants.values()]

        changes = []
        for rid in rng.sample(sorted(s.restaurants), k=max(1, len(s.restaurants) // 50)):
            r = dict(s.restaurants[rid])
            roll = rng.random()
            if roll < 0.6:
                r["commission_pct"] = round(min(30.0, max(10.0, r["commission_pct"] + rng.choice([-2, 2, 3]))), 1)
            elif roll < 0.85:
                r["is_active"] = not r["is_active"]
            else:
                r["cuisine"] = rng.choice(CUISINES)
            r["updated_at"] = _iso(now + timedelta(seconds=rng.randint(0, self.batch_minutes * 60 - 1)))
            s.restaurants[rid] = r
            changes.append(r)
        return changes

    def _free_rider(self, rng, city, zone, at: datetime) -> str | None:
        def free(rid):
            return all(_parse(e) <= at for _, e in self.state.rider_trips.get(rid, []))

        local = [r for r, cz in self.riders.items() if cz == (city, zone) and free(r)]
        if local:
            return rng.choice(local)
        nearby = [r for r, cz in self.riders.items() if cz[0] == city and free(r)]
        return rng.choice(nearby) if nearby else None

    # ------------------------------------------------------------- one batch
    def next_batch(self) -> dict:
        s = self.state
        batch_id = s.next_batch
        rng = random.Random(self.seed * 100_003 + batch_id)
        start = _parse(s.clock)
        end = start + timedelta(minutes=self.batch_minutes)

        restaurants = self._restaurant_changes(rng, start)
        active = {}
        for r in s.restaurants.values():
            if r["is_active"]:
                active.setdefault((r["city"], r["zone"]), []).append(r)

        # ---- orders and their full lifecycle
        n_orders = int(self.base_orders_per_batch * demand_multiplier(start) * rng.uniform(0.85, 1.15))
        placed_times = sorted(
            start + timedelta(seconds=rng.uniform(0, self.batch_minutes * 60)) for _ in range(n_orders)
        )
        new_events = []
        for placed in placed_times:
            city, zone = rng.choice(self.zones)
            if not active.get((city, zone)):
                continue
            rest = rng.choice(active[(city, zone)])
            s.order_seq += 1
            new_tip_app = batch_id >= self.tip_rollout_batch and rng.random() < 0.6
            base = {
                "order_id": f"ORD{s.order_seq:09d}",
                "customer_id": f"CUST{rng.randint(1, 25000):06d}",
                "restaurant_id": rest["restaurant_id"],
                "city": city,
                "zone": zone,
                "payment_mode": rng.choices(PAYMENT_MODES, weights=[55, 20, 15, 10])[0],
                "order_value": round(min(4500, rng.lognormvariate(5.8, 0.5)), 2),
                "delivery_fee": rng.choice([0, 19, 25, 35, 49]),
                "app_version": "5.2.0" if new_tip_app else "5.1.4",
            }
            if new_tip_app:
                base["tip_amount"] = rng.choice([0, 0, 0, 10, 20, 30, 50])

            def ev(status, ts, rider=None, reason=None):
                e = dict(
                    base,
                    event_id=self._uuid(rng),
                    status=status,
                    event_ts=_iso(ts),
                    rider_id=rider,
                    cancel_reason=reason,
                )
                if rng.random() < 0.05:  # some clients send lower-case statuses
                    e["status"] = status.lower()
                return e

            new_events.append(ev("PLACED", placed))
            roll = rng.random()
            if roll < 0.035:
                new_events.append(
                    ev("CANCELLED", placed + timedelta(minutes=rng.uniform(0.5, 3)), reason="CUSTOMER_CANCELLED")
                )
                continue
            accepted = placed + timedelta(minutes=rng.uniform(0.3, 3.5))
            if roll < 0.055:
                new_events.append(ev("CANCELLED", accepted, reason="RESTAURANT_REJECTED"))
                continue
            rider = self._free_rider(rng, city, zone, accepted)
            if rider is None:
                new_events.append(
                    ev("CANCELLED", accepted + timedelta(minutes=rng.uniform(3, 8)), reason="NO_RIDER_AVAILABLE")
                )
                continue
            prep_mean = {"Biryani": 16, "Pizza": 14, "Desserts": 7, "Healthy": 10}.get(rest["cuisine"], 12)
            picked = accepted + timedelta(minutes=max(4, rng.gauss(prep_mean, 4)))
            delivered = picked + timedelta(minutes=max(5, rng.gauss(17, 6)))
            s.rider_trips.setdefault(rider, []).append([_iso(accepted), _iso(delivered)])
            new_events += [
                ev("ACCEPTED", accepted, rider),
                ev("PICKED_UP", picked, rider),
                ev("DELIVERED", delivered, rider),
            ]

        # ---- decide what "arrives" in this batch's file
        candidates = s.pending + new_events
        arrived, still_pending = [], []
        for e in candidates:
            due = _parse(e["event_ts"]) < end
            held_back = due and e.get("_held") is None and rng.random() < 0.02  # late arrival
            if due and not held_back:
                e.pop("_held", None)
                arrived.append(e)
            else:
                if held_back:
                    e["_held"] = True
                still_pending.append(e)
        s.pending = still_pending

        arrived += self._dirty_order_events(rng, arrived)

        # ---- rider GPS pings every 2 minutes
        pings = []
        for rider, (city, zone) in self.riders.items():
            trips = [(_parse(a), _parse(b)) for a, b in s.rider_trips.get(rider, [])]
            t = start + timedelta(seconds=rng.uniform(0, 120))
            while t < end:
                on_trip = any(a <= t < b for a, b in trips)
                lat, lon = self._zone_point(rng, city, zone)
                pings.append(
                    {
                        "rider_id": rider,
                        "city": city,
                        "zone": zone,
                        "lat": lat,
                        "lon": lon,
                        "rider_status": "ON_TRIP" if on_trip else "IDLE",
                        "battery_pct": rng.randint(8, 100),
                        "ping_ts": _iso(t),
                    }
                )
                t += timedelta(seconds=120)
        for p in rng.sample(pings, k=int(len(pings) * self.dirty_rate / 3)):
            p["lat"], p["lon"] = 0.0, 0.0  # GPS glitch

        # ---- prune finished trips and advance the clock
        s.rider_trips = {
            r: [t for t in ts if _parse(t[1]) > end - timedelta(hours=2)] for r, ts in s.rider_trips.items()
        }
        s.rider_trips = {r: ts for r, ts in s.rider_trips.items() if ts}
        s.clock = _iso(end)
        s.next_batch += 1
        return {
            "batch_id": batch_id,
            "window_start": _iso(start),
            "window_end": _iso(end),
            "order_events": arrived,
            "rider_pings": pings,
            "restaurants": restaurants,
        }

    def _dirty_order_events(self, rng: random.Random, events: list[dict]) -> list[dict]:
        """Duplicates and invalid records, roughly `dirty_rate` of the volume."""
        extra = []
        k = int(len(events) * self.dirty_rate)
        for e in rng.sample(events, k=min(k, len(events))):
            extra.append(dict(e))  # exact duplicate: same event_id (client retry)
        for e in rng.sample(events, k=min(k // 2, len(events))):
            bad = dict(e, event_id=self._uuid(rng))
            fault = rng.choice(["null_order", "negative_value", "unknown_status", "null_restaurant"])
            if fault == "null_order":
                bad["order_id"] = None
            elif fault == "negative_value":
                bad["order_value"] = -abs(bad["order_value"])
            elif fault == "unknown_status":
                bad["status"] = "REFUNDED_PENDING"
            else:
                bad["restaurant_id"] = None
            extra.append(bad)
        return extra


def write_batch(batch: dict, landing_root: str) -> dict:
    """Write one batch as JSON Lines files, one folder per feed.

    Files are written under a `_tmp` name and renamed, so Auto Loader never
    picks up a half-written file (it ignores names starting with `_`).
    """
    written = {}
    for feed in ("order_events", "rider_pings", "restaurants"):
        rows = batch[feed]
        if not rows:
            continue
        folder = os.path.join(landing_root, feed)
        os.makedirs(folder, exist_ok=True)
        name = f"{feed}_{batch['batch_id']:05d}.json"
        tmp = os.path.join(folder, "_tmp_" + name)
        with open(tmp, "w") as fh:
            for row in rows:
                fh.write(json.dumps(row) + "\n")
        os.replace(tmp, os.path.join(folder, name))
        written[feed] = len(rows)
    return written
