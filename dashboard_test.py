import json

from db import DB


PREFIX = {
    "EscrowCreated": "ec",
    "ShipmentLinked" : "lk",
    "EscrowExtended" : "ex",
    "EscrowCancelled" : "cn",
    "EscrowExpired" : "xp",
    "FundsRefunded" : "rf",
    "FundsReleased" : "rl",
}
def test_insert_multiple_escrow():
    db = DB()
    with open("test_escrows.json") as f:
        test_data = json.loads(f.read())
    for pref in list(PREFIX.keys()):
        for i in range(3):
            try:
                data = test_data[pref][i]
                db.put(f"{PREFIX[pref]}:{test_data[pref][i]["escrow_id"]}", json.dumps(data))
            except Exception:
                continue
    