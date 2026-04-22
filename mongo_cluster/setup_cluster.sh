#!/usr/bin/env bash
# setup_cluster.sh — ONE-TIME setup for the sharded MongoDB cluster.
# Run from the project root:  bash mongo_cluster/setup_cluster.sh
#
# Port layout (avoids the MongoDB Windows service on :27017):
#   Config server  (configRS): 28100
#   Shard 1 (rs1):  28017, 28018, 28019
#   Shard 2 (rs2):  28021, 28022, 28023
#   Shard 3 (rs3):  28024, 28025, 28026
#   mongos router:  28020   ← app connects here

MONGOD="/c/Program Files/MongoDB/Server/8.2/bin/mongod.exe"
MONGOS="/c/Program Files/MongoDB/Server/8.2/bin/mongos.exe"
PYTHON="C:/Python314/python.exe"

echo "======================================================"
echo "  MongoDB Sharded Cluster — One-Time Setup"
echo "======================================================"

# ── Step 1: Wipe and recreate data directories ────────────────────────────────
echo ""
echo "[STEP 1]  Wiping stale data and recreating directories ..."
rm -rf /c/data/configdb  \
       /c/data/shard1-1 /c/data/shard1-2 /c/data/shard1-3 \
       /c/data/shard2-1 /c/data/shard2-2 /c/data/shard2-3 \
       /c/data/shard3-1 /c/data/shard3-2 /c/data/shard3-3
mkdir -p /c/data/configdb
mkdir -p /c/data/shard1-1 /c/data/shard1-2 /c/data/shard1-3
mkdir -p /c/data/shard2-1 /c/data/shard2-2 /c/data/shard2-3
mkdir -p /c/data/shard3-1 /c/data/shard3-2 /c/data/shard3-3
mkdir -p /c/data/logs
echo "         Done."

# ── Step 2: Config server (port 28100) ────────────────────────────────────────
echo ""
echo "[STEP 2]  Starting config server (configRS @ 28100) ..."
"$MONGOD" --configsvr --replSet configRS --port 28100 \
    --dbpath "C:/data/configdb" \
    >> /c/data/logs/configsvr.log 2>&1 &
echo "         Waiting 8s ..."
sleep 8

# ── Step 3: 9 shard mongod instances ──────────────────────────────────────────
echo ""
echo "[STEP 3]  Starting 9 shard mongod instances ..."

# Shard 1 (rs1) — ports 28017-28019
"$MONGOD" --shardsvr --replSet rs1 --port 28017 --dbpath "C:/data/shard1-1" >> /c/data/logs/shard1-1.log 2>&1 &
"$MONGOD" --shardsvr --replSet rs1 --port 28018 --dbpath "C:/data/shard1-2" >> /c/data/logs/shard1-2.log 2>&1 &
"$MONGOD" --shardsvr --replSet rs1 --port 28019 --dbpath "C:/data/shard1-3" >> /c/data/logs/shard1-3.log 2>&1 &

# Shard 2 (rs2) — ports 28021-28023
"$MONGOD" --shardsvr --replSet rs2 --port 28021 --dbpath "C:/data/shard2-1" >> /c/data/logs/shard2-1.log 2>&1 &
"$MONGOD" --shardsvr --replSet rs2 --port 28022 --dbpath "C:/data/shard2-2" >> /c/data/logs/shard2-2.log 2>&1 &
"$MONGOD" --shardsvr --replSet rs2 --port 28023 --dbpath "C:/data/shard2-3" >> /c/data/logs/shard2-3.log 2>&1 &

# Shard 3 (rs3) — ports 28024-28026
"$MONGOD" --shardsvr --replSet rs3 --port 28024 --dbpath "C:/data/shard3-1" >> /c/data/logs/shard3-1.log 2>&1 &
"$MONGOD" --shardsvr --replSet rs3 --port 28025 --dbpath "C:/data/shard3-2" >> /c/data/logs/shard3-2.log 2>&1 &
"$MONGOD" --shardsvr --replSet rs3 --port 28026 --dbpath "C:/data/shard3-3" >> /c/data/logs/shard3-3.log 2>&1 &

echo "         Waiting 15s ..."
sleep 15

# ── Step 4: mongos router (port 28020) ────────────────────────────────────────
echo ""
echo "[STEP 4]  Starting mongos router (@ 28020) ..."
"$MONGOS" --configdb "configRS/localhost:28100" --port 28020 \
    >> /c/data/logs/mongos.log 2>&1 &
echo "         Waiting 6s ..."
sleep 6

# ── Step 5: Init replica sets + configure sharding ────────────────────────────
echo ""
echo "[STEP 5]  Initialising replica sets and configuring sharding ..."
"$PYTHON" "$(dirname "$0")/init_cluster.py"

echo ""
echo "======================================================"
echo "  Setup complete!"
echo "  App connects to:  mongodb://localhost:28020/"
echo "  Daily startup:    bash mongo_cluster/start_cluster.sh"
echo "======================================================"
