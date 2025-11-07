from enum import Enum
from typing import Dict, Optional, List, Tuple
from db import DB
import json

class EscrowType(Enum):
    EXPIRED = 0
    CANCELLED = 1
    LINKED = 2
    EXTENDED = 3
    CREATED = 4
    REFUNDED = 5
    RELEASED = 6

PREFIX = {
    "ec" : "EscrowCreated",
    "lk" : "ShipmentLinked",
    "ex" : "EscrowExtended",
    "cn" : "EscrowCancelled",
    "xp" : "EscrowExpired",
    "rf" : "FundsRefunded",
    "rl" : "FundsReleased",
}

LATEST_ORDER = ["rf", "rl", "xp", "ex", "lk", "cn", "ec"]

class Storage:
    def __init__(self, db: DB = None):
        self.db = db if db else DB()
        self.states = list(PREFIX.keys())

    def get_escrow_by_id(self, escrow_id: int) -> Dict[str, str]:
        keys = [f"{state}:{escrow_id}" for state in self.states]
        result = {}
        for key in keys:
            try:
                val = self.db.get(key)
                if val is not None:
                    result[key] = val
            except Exception:
                pass
        return result

    def get_latest(self, escrow_id: int) -> Optional[Tuple[str, Dict]]:
        data = self.get_escrow_by_id(escrow_id)
        if not data:
            return None
        for p in LATEST_ORDER:
            key = f"{p}:{escrow_id}"
            if key in data:
                try:
                    decoded = json.loads(data[key])
                except Exception:
                    decoded = {"raw": data[key]}
                return (p, decoded)
        return None

    def iterate_prefix(self, prefix: str):
        return self.db.iterate(prefix)  # yields (key, value)

    def get_latest_all(self, limit: int = 100) -> List[Dict]:
        # Strategy: iterate all prefixes, group by escrow_id, pick latest by LATEST_ORDER
        buckets: Dict[int, Dict[str, str]] = {}
        for pref in self.states:
            for key, val in self.db.iterate(f"{pref}:"):
                try:
                    _, sid = key.split(":")
                    eid = int(sid)
                except Exception:
                    continue
                d = buckets.setdefault(eid, {})
                d[pref] = val

        latest_list = []
        for eid, states in buckets.items():
            for p in LATEST_ORDER:
                if p in states:
                    try:
                        payload = json.loads(states[p])
                    except Exception:
                        payload = {"raw": states[p]}
                    latest_list.append({
                        "escrow_id": eid,
                        "prefix": p,
                        "event": PREFIX[p],
                        "data": payload
                    })
                    break

        # Sort: terminal first (rf/rl), then by escrow_id desc; cap limit
        latest_list.sort(key=lambda x: (LATEST_ORDER.index(x["prefix"]), -x["escrow_id"]))
        return latest_list[:limit]
    
    