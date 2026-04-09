"""
transform_runner.py
Transform Orchestrator -- Transform Service
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Reads raw source-format files from data/raw/, runs each adapter,
and writes normalised canonical JSON files to data/ replacing the old
synthetic data files.

Output files (same paths the Kafka producer reads):
    data/earthquakes.json   ← from USGS GeoJSON
    data/floods.json        ← from USGS HWM (grouped by event_id)
    data/fires.json         ← from NASA FIRMS MODIS + VIIRS (merged)
    data/storms.json        ← from OpenWeather (storm-gated)

Each output record has the canonical pipeline schema:
    event_id, event_type, severity_raw, latitude, longitude,
    timestamp, source
  plus extended fields (magnitude, area_km2, disaster-specific).

Usage:
    python transform_runner.py                   # uses default paths
    python transform_runner.py --raw-dir ../../data/raw --out-dir ../../data
    python transform_runner.py --dry-run         # print stats, write nothing
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("TransformRunner")

# ── Resolve default paths relative to this file ───────────────────────────────
_SERVICE_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.normpath(os.path.join(_SERVICE_DIR, "..", ".."))
_DEFAULT_RAW = os.path.join(_PROJECT_ROOT, "data", "raw")
_DEFAULT_OUT = os.path.join(_PROJECT_ROOT, "data")

# ── Adapter imports (local to this service) ───────────────────────────────────
sys.path.insert(0, _SERVICE_DIR)
from adapters import usgs_earthquake, usgs_flood, nasa_firms_modis, nasa_firms_viirs, openweather


# ── Helpers ───────────────────────────────────────────────────────────────────

def _write(path: str, events: list[dict], dry_run: bool) -> None:
    if dry_run:
        log.info("[DRY RUN] Would write %d events → %s", len(events), path)
        return
    with open(path, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2, ensure_ascii=False)
    size_kb = os.path.getsize(path) / 1024
    log.info("Written %d events → %s  (%.1f KB)", len(events), path, size_kb)

def _severity_dist(events: list[dict]) -> str:
    from collections import Counter
    # The consumer applies thresholds — here we just inspect severity_raw ranges
    c: Counter = Counter()
    for e in events:
        raw = e.get("severity_raw", 0)
        c[_classify(e["event_type"], raw)] += 1
    return "  ".join(f"{k}={v}" for k, v in sorted(c.items()))

def _classify(event_type: str, raw: float) -> str:
    """Mirror of consumer thresholds — for reporting only."""
    thresholds = {
        "earthquake": (1.5, 3.0, 5.0, 7.0),
        "fire":       (5.0, 20.0, 50.0, 200.0),
        "flood":      (0.5, 1.0, 2.0, 3.0),
        "storm":      (35.0, 62.0, 120.0, 200.0),
    }
    t = thresholds.get(event_type, (0, 1, 2, 3))
    if raw < t[0]:   return "DROP"
    if raw >= t[3]:  return "CRITICAL"
    if raw >= t[2]:  return "HIGH"
    if raw >= t[1]:  return "MEDIUM"
    return "LOW"


# ── Main ──────────────────────────────────────────────────────────────────────

def run(raw_dir: str, out_dir: str, dry_run: bool) -> None:
    os.makedirs(out_dir, exist_ok=True)

    # ── Earthquakes ──────────────────────────────────────────────────────────
    eq_path = os.path.join(raw_dir, "usgs_earthquakes.geojson")
    log.info("[EQ]   Reading %s ...", eq_path)
    earthquakes = usgs_earthquake.transform(eq_path)
    log.info("[EQ]   %d events produced  |  %s", len(earthquakes), _severity_dist(earthquakes))
    _write(os.path.join(out_dir, "earthquakes.json"), earthquakes, dry_run)

    # ── Floods ───────────────────────────────────────────────────────────────
    fl_path = os.path.join(raw_dir, "usgs_floods.json")
    log.info("[FL]   Reading %s ...", fl_path)
    floods = usgs_flood.transform(fl_path)
    log.info("[FL]   %d events produced  |  %s", len(floods), _severity_dist(floods))
    _write(os.path.join(out_dir, "floods.json"), floods, dry_run)

    # ── Fires: MODIS + VIIRS merged ──────────────────────────────────────────
    modis_path = os.path.join(raw_dir, "nasa_firms_modis.csv")
    viirs_path = os.path.join(raw_dir, "nasa_firms_viirs.csv")
    log.info("[FIRE] Reading MODIS: %s ...", modis_path)
    fires_modis = nasa_firms_modis.transform(modis_path)
    log.info("[FIRE] Reading VIIRS: %s ...", viirs_path)
    fires_viirs = nasa_firms_viirs.transform(viirs_path)
    fires = fires_modis + fires_viirs
    log.info("[FIRE] %d total fire events (MODIS=%d, VIIRS=%d)  |  %s",
             len(fires), len(fires_modis), len(fires_viirs), _severity_dist(fires))
    _write(os.path.join(out_dir, "fires.json"), fires, dry_run)

    # ── Storms ───────────────────────────────────────────────────────────────
    st_path = os.path.join(raw_dir, "openweather_storms.json")
    log.info("[ST]   Reading %s ...", st_path)
    storms = openweather.transform(st_path)
    log.info("[ST]   %d events produced  |  %s", len(storms), _severity_dist(storms))
    _write(os.path.join(out_dir, "storms.json"), storms, dry_run)

    # ── Summary ──────────────────────────────────────────────────────────────
    total = len(earthquakes) + len(floods) + len(fires) + len(storms)
    print("\n" + "=" * 62)
    print("  Transform complete")
    print("=" * 62)
    print(f"  Earthquakes : {len(earthquakes):>5}  (source: USGS_EQ)")
    print(f"  Floods      : {len(floods):>5}  (source: USGS_FLOOD)")
    print(f"  Fires       : {len(fires):>5}  (MODIS={len(fires_modis)}, VIIRS={len(fires_viirs)})")
    print(f"  Storms      : {len(storms):>5}  (source: OPENWEATHER)")
    print(f"  {'-'*40}")
    print(f"  TOTAL       : {total:>5}  canonical events")
    if dry_run:
        print("\n  [DRY RUN] No files written.")
    else:
        print(f"\n  Output dir  : {out_dir}")
    print("=" * 62 + "\n")


def main() -> None:
    p = argparse.ArgumentParser(
        description="Transform raw source data to canonical pipeline format"
    )
    p.add_argument("--raw-dir", default=_DEFAULT_RAW,
                   help=f"Directory containing raw source files (default: {_DEFAULT_RAW})")
    p.add_argument("--out-dir", default=_DEFAULT_OUT,
                   help=f"Output directory for canonical JSON files (default: {_DEFAULT_OUT})")
    p.add_argument("--dry-run", action="store_true",
                   help="Print stats without writing output files")
    args = p.parse_args()

    print("\n" + "=" * 62)
    print("  Disaster Monitoring System -- Transform Service")
    print("=" * 62)
    print(f"  Raw dir  : {args.raw_dir}")
    print(f"  Out dir  : {args.out_dir}")
    print(f"  Dry run  : {args.dry_run}")
    print("=" * 62 + "\n")

    run(raw_dir=args.raw_dir, out_dir=args.out_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
