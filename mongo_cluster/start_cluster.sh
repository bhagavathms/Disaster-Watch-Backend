#!/usr/bin/env bash
# start_cluster.sh — daily startup (run after setup_cluster.sh was done once).
#
# Port layout:
#   Config server  (configRS): 28100
#   Shard 1 (rs1):  28017, 28018, 28019
#   Shard 2 (rs2):  28021, 28022, 28023
#   Shard 3 (rs3):  28024, 28025, 28026
#   mongos router:  28020   ← app connects here

MONGOD="/c/Program Files/MongoDB/Server/8.2/bin/mongod.exe"
MONGOS="/c/Program Files/MongoDB/Server/8.2/bin/mongos.exe"

echo "======================================================"
echo "  Starting MongoDB Sharded Cluster"
echo "======================================================"

echo "[1/5]  Config server (configRS @ 28100) ..."
"$MONGOD" --configsvr --replSet configRS --port 28100 \
    --dbpath "C:/data/configdb" \
    >> /c/data/logs/configsvr.log 2>&1 &
sleep 6

echo "[2/5]  Shard 1 (rs1 @ 28017, 28018, 28019) ..."
"$MONGOD" --shardsvr --replSet rs1 --port 28017 --dbpath "C:/data/shard1-1" >> /c/data/logs/shard1-1.log 2>&1 &
"$MONGOD" --shardsvr --replSet rs1 --port 28018 --dbpath "C:/data/shard1-2" >> /c/data/logs/shard1-2.log 2>&1 &
"$MONGOD" --shardsvr --replSet rs1 --port 28019 --dbpath "C:/data/shard1-3" >> /c/data/logs/shard1-3.log 2>&1 &

echo "[3/5]  Shard 2 (rs2 @ 28021, 28022, 28023) ..."
"$MONGOD" --shardsvr --replSet rs2 --port 28021 --dbpath "C:/data/shard2-1" >> /c/data/logs/shard2-1.log 2>&1 &
"$MONGOD" --shardsvr --replSet rs2 --port 28022 --dbpath "C:/data/shard2-2" >> /c/data/logs/shard2-2.log 2>&1 &
"$MONGOD" --shardsvr --replSet rs2 --port 28023 --dbpath "C:/data/shard2-3" >> /c/data/logs/shard2-3.log 2>&1 &

echo "[4/5]  Shard 3 (rs3 @ 28024, 28025, 28026) ..."
"$MONGOD" --shardsvr --replSet rs3 --port 28024 --dbpath "C:/data/shard3-1" >> /c/data/logs/shard3-1.log 2>&1 &
"$MONGOD" --shardsvr --replSet rs3 --port 28025 --dbpath "C:/data/shard3-2" >> /c/data/logs/shard3-2.log 2>&1 &
"$MONGOD" --shardsvr --replSet rs3 --port 28026 --dbpath "C:/data/shard3-3" >> /c/data/logs/shard3-3.log 2>&1 &

echo "       Waiting 12s for shard instances ..."
sleep 12

echo "[5/5]  mongos router (@ 28020) ..."
"$MONGOS" --configdb "configRS/localhost:28100" --port 28020 \
    >> /c/data/logs/mongos.log 2>&1 &
sleep 5

echo ""
echo "  Cluster ready.  Connect to: mongodb://localhost:28020/"
echo "======================================================"
