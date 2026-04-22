#!/usr/bin/env bash
# stop_cluster.sh — gracefully shuts down all cluster processes

MONGOD="/c/Program Files/MongoDB/Server/8.2/bin/mongod.exe"
PYTHON="C:/Python314/python.exe"

echo "Shutting down MongoDB cluster ..."

"$PYTHON" - <<'EOF'
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

ports = {
    "mongos   (28020)": 28020,
    "shard1-1 (28017)": 28017,
    "shard1-2 (28018)": 28018,
    "shard1-3 (28019)": 28019,
    "shard2-1 (28021)": 28021,
    "shard2-2 (28022)": 28022,
    "shard2-3 (28023)": 28023,
    "shard3-1 (28024)": 28024,
    "shard3-2 (28025)": 28025,
    "shard3-3 (28026)": 28026,
    "configsvr (28100)": 28100,
}

for label, port in ports.items():
    try:
        c = MongoClient("localhost", port, serverSelectionTimeoutMS=1500)
        c.admin.command("shutdown", force=True)
        print(f"  [stopped]  {label}")
    except Exception:
        print(f"  [skipped]  {label}  (not running)")
EOF

echo "Done."
