"""
main.py — Step 1 Entry Point
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Orchestrates the full data-generation pipeline:
  Step 1  generate_data.py   → raw source-schema records (USGS / NASA FIRMS / USGS HWM / IBTrACS)
  Step 2  normalizer.py      → unified Kafka event schema
  Step 3  writer_service.py  → JSON files on disk  (data/*.json)

Run:
    python main.py                  # default ~400 events
    python main.py --scale 100      # ~40 000 events
    python main.py --scale 1000     # ~400 000 events  (big-data demo)
    python main.py --skip-generate  # reuse existing data/raw/ files, only normalize + write
    python main.py --skip-normalize # reuse existing data/*.json files  (skip to Kafka)

Output layout:
    data/
      raw/
        earthquakes.json   ← USGS GeoJSON Feature array
        fires.json         ← NASA FIRMS MODIS record array
        floods.json        ← USGS HWM record array
        storms.json        ← IBTrACS record array
      earthquakes.json     ← unified Kafka schema
      fires.json
      floods.json
      storms.json
"""

import argparse
import json
import logging
from pathlib import Path

from generate_data import generate_all_events
from normalizer import normalize_all, normalize_from_files
from writer_service import DatasetWriterService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_BASE = dict(n_earthquakes=100, n_fires=105, n_floods=105, n_storms=95)

_RAW_FILE_MAP = {
    "earthquake": "earthquakes.json",
    "fire":       "fires.json",
    "flood":      "floods.json",
    "storm":      "storms.json",
}


def _write_raw(raw_datasets: dict, raw_dir: Path) -> None:
    """Persist raw source-schema records to data/raw/."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    for event_type, records in raw_datasets.items():
        path = raw_dir / _RAW_FILE_MAP[event_type]
        with path.open("w", encoding="utf-8") as fh:
            json.dump(records, fh, indent=2, ensure_ascii=False)
        print(f"  [raw/{event_type:<12}]  {path}  ({len(records):,} records)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic disaster events and normalize to unified schema.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python main.py                   # generate raw + normalize + write\n"
            "  python main.py --scale 1000      # ~400 000 events\n"
            "  python main.py --skip-generate   # normalize existing data/raw/ files\n"
        ),
    )
    parser.add_argument(
        "--scale", type=int, default=1, metavar="N",
        help="Multiply base event counts by N (default: 1 → ~400 events)",
    )
    parser.add_argument(
        "--skip-generate", action="store_true",
        help="Skip generation; load existing raw files from data/raw/ and normalize",
    )
    parser.add_argument(
        "--skip-normalize", action="store_true",
        help="Skip generation and normalization; use existing data/*.json (Kafka-ready)",
    )
    args = parser.parse_args()

    if args.scale < 1:
        parser.error("--scale must be >= 1")

    raw_dir  = Path("data") / "raw"
    data_dir = Path("data")

    print("\n" + "=" * 64)
    print("  Disaster Monitoring System — Data Generation Pipeline")
    print("=" * 64)

    # ── Step 1: Generate raw source-schema records ────────────────────────────
    if args.skip_normalize:
        print("\n  [SKIP]  Generation + normalization — using existing data/*.json")
        print("\n" + "=" * 64)
        print("  Step 1 complete (skipped).  data/*.json already present.")
        print("=" * 64 + "\n")
        return

    if args.skip_generate:
        print(f"\n  [SKIP]  Generation — loading raw files from {raw_dir}")
        print("\n[2/3]  Normalizing raw records …")
        unified = normalize_from_files(raw_dir)
    else:
        total_approx = sum(_BASE.values()) * args.scale
        if args.scale > 1:
            print(f"\n  Scale factor : {args.scale}×  (~{total_approx:,} events)")
        if args.scale >= 500:
            print("  WARNING: large scale — may take several minutes.")

        print("\n[1/3]  Generating raw source-schema records …")
        raw_datasets = generate_all_events(
            n_earthquakes=_BASE["n_earthquakes"] * args.scale,
            n_fires=      _BASE["n_fires"]       * args.scale,
            n_floods=     _BASE["n_floods"]      * args.scale,
            n_storms=     _BASE["n_storms"]      * args.scale,
        )
        for etype, records in raw_datasets.items():
            print(f"  {etype:<12}  {len(records):>7,} raw records")

        print(f"\n  Writing raw files to {raw_dir} …")
        _write_raw(raw_datasets, raw_dir)

        # ── Step 2: Normalize to unified Kafka schema ─────────────────────────
        print("\n[2/3]  Normalizing to unified Kafka schema …")
        unified = normalize_all(raw_datasets)

    # ── Step 3: Write unified files via the Dataset Writer Microservice ───────
    print("\n[3/3]  Writing unified datasets to disk …\n")
    writer = DatasetWriterService(output_dir=str(data_dir))
    writer.print_summary(unified)
    paths = writer.write_all(unified)

    print("\nUnified output files:")
    for event_type, path in paths.items():
        print(f"  [{event_type:<12}]  {path}")

    print("\n" + "=" * 64)
    print("  Step 1 complete.")
    print("  Next step: Kafka producer will stream events from data/")
    print("=" * 64 + "\n")


if __name__ == "__main__":
    main()
