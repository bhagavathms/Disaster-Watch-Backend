"""
init_cluster.py
Initialises the MongoDB sharded cluster using pymongo (no mongosh needed).

Run automatically by setup_cluster.sh after all mongod/mongos processes start.
Do NOT run this again after setup — it is idempotent but will print errors
for already-initialised replica sets.
"""

import time
import sys

try:
    from pymongo import MongoClient
    from pymongo.errors import OperationFailure, ServerSelectionTimeoutError
except ImportError:
    print("ERROR: pymongo not installed. Run: pip install pymongo")
    sys.exit(1)


def _wait_for(host: str, port: int, label: str, retries: int = 20) -> MongoClient:
    """Poll until the mongod/mongos at host:port accepts connections."""
    for attempt in range(1, retries + 1):
        try:
            c = MongoClient(
                host, port,
                serverSelectionTimeoutMS=3000,
                directConnection=True,   # skip RS discovery on uninitiated nodes
            )
            c.admin.command("ping")
            print(f"    [{label}]  ready on port {port}")
            return c
        except Exception:
            print(f"    [{label}]  waiting ... (attempt {attempt}/{retries})")
            time.sleep(2)
    print(f"ERROR: {label} on port {port} did not become ready in time.")
    sys.exit(1)


def _init_rs(port: int, rs_id: str, members: list[dict]) -> None:
    """Send replSetInitiate to the given port."""
    _wait_for("localhost", port, rs_id)
    # Give the node a moment to finish all startup tasks after ping succeeds
    time.sleep(3)
    client = MongoClient(
        f"localhost:{port}",
        directConnection=True,
        serverSelectionTimeoutMS=5000,
    )
    config = {"_id": rs_id, "members": members}
    if rs_id == "configRS":
        config["configsvr"] = True
    try:
        client.admin.command({"replSetInitiate": config})
        print(f"    [{rs_id}]  replica set initiated")
    except OperationFailure as e:
        msg = str(e).lower()
        if "already initialized" in msg or "already initialised" in msg or "already init" in msg:
            print(f"    [{rs_id}]  already initialised — skipping")
        else:
            raise


def _wait_for_primary(port: int, rs_id: str, retries: int = 15) -> MongoClient:
    """Wait until the replica set has elected a primary."""
    for attempt in range(1, retries + 1):
        try:
            c = MongoClient(
                f"localhost:{port}",
                replicaSet=rs_id,
                serverSelectionTimeoutMS=2000,
                directConnection=False,
            )
            status = c.admin.command("replSetGetStatus")
            primaries = [m for m in status["members"] if m["stateStr"] == "PRIMARY"]
            if primaries:
                print(f"    [{rs_id}]  primary elected: {primaries[0]['name']}")
                return c
        except Exception:
            pass
        print(f"    [{rs_id}]  waiting for primary ... (attempt {attempt}/{retries})")
        time.sleep(2)
    print(f"ERROR: [{rs_id}] no primary elected in time.")
    sys.exit(1)


def main() -> None:
    print("\n--- Initialising Config Server Replica Set ---")
    _init_rs(
        port=28100,
        rs_id="configRS",
        members=[{"_id": 0, "host": "localhost:28100"}],
    )
    time.sleep(3)

    print("\n--- Initialising Shard 1 Replica Set (rs1) ---")
    _init_rs(
        port=28017,
        rs_id="rs1",
        members=[
            {"_id": 0, "host": "localhost:28017"},
            {"_id": 1, "host": "localhost:28018"},
            {"_id": 2, "host": "localhost:28019"},
        ],
    )

    print("\n--- Initialising Shard 2 Replica Set (rs2) ---")
    _init_rs(
        port=28021,
        rs_id="rs2",
        members=[
            {"_id": 0, "host": "localhost:28021"},
            {"_id": 1, "host": "localhost:28022"},
            {"_id": 2, "host": "localhost:28023"},
        ],
    )

    print("\n--- Initialising Shard 3 Replica Set (rs3) ---")
    _init_rs(
        port=28024,
        rs_id="rs3",
        members=[
            {"_id": 0, "host": "localhost:28024"},
            {"_id": 1, "host": "localhost:28025"},
            {"_id": 2, "host": "localhost:28026"},
        ],
    )

    print("\n--- Waiting for primaries to be elected (30s) ---")
    time.sleep(30)
    _wait_for_primary(28017, "rs1")
    _wait_for_primary(28021, "rs2")
    _wait_for_primary(28024, "rs3")

    print("\n--- Configuring sharding via mongos (28020) ---")
    mongos = _wait_for("localhost", 28020, "mongos")

    # Register all 3 shards
    for rs_id, hosts in [
        ("rs1", "localhost:28017,localhost:28018,localhost:28019"),
        ("rs2", "localhost:28021,localhost:28022,localhost:28023"),
        ("rs3", "localhost:28024,localhost:28025,localhost:28026"),
    ]:
        try:
            result = mongos.admin.command("addShard", f"{rs_id}/{hosts}")
            print(f"    [mongos]  shard added: {result.get('shardAdded', rs_id)}")
        except OperationFailure as e:
            if "already" in str(e).lower():
                print(f"    [mongos]  {rs_id} already registered — skipping")
            else:
                raise

    # Enable sharding on disaster_db
    try:
        mongos.admin.command("enableSharding", "disaster_db")
        print("    [mongos]  sharding enabled on database: disaster_db")
    except OperationFailure as e:
        if "already" in str(e).lower():
            print("    [mongos]  disaster_db sharding already enabled")
        else:
            raise

    # Shard the collection on hashed _id for even distribution across 3 shards
    try:
        mongos.admin.command(
            "shardCollection",
            "disaster_db.disaster_events",
            key={"_id": "hashed"},
        )
        print("    [mongos]  collection sharded: disaster_db.disaster_events  key={_id: 'hashed'}")
    except OperationFailure as e:
        if "already" in str(e).lower():
            print("    [mongos]  collection already sharded")
        else:
            raise

    print("\n--- Verifying shard distribution ---")
    time.sleep(2)
    try:
        status = mongos.admin.command("listShards")
        print(f"    Active shards: {len(status['shards'])}")
        for s in status["shards"]:
            print(f"      {s['_id']:10s}  {s['host']}")
    except Exception as e:
        print(f"    WARNING: could not list shards: {e}")

    print("\n  Cluster initialisation complete.")
    print("  Connect Compass to:  mongodb://localhost:28020/")


if __name__ == "__main__":
    main()
