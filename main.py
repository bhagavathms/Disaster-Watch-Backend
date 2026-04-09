"""
main.py — Step 1 Entry Point
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Orchestrates:
  Task A — Synthetic event generation  (generate_data.py)
  Task B — Segmentation by event_type  (generate_data.py)
  Task C — Persist to JSON files       (writer_service.py)

Run:
    python main.py

Output:
    data/
      earthquakes.json
      fires.json
      floods.json
      storms.json
"""

from generate_data import generate_all_events
from writer_service import DatasetWriterService


def main() -> None:
    print("\n" + "=" * 60)
    print("  Disaster Monitoring System -- Step 1")
    print("  Synthetic Dataset Generation & Storage")
    print("=" * 60)

    # ── Task A + B: Generate & segment synthetic events ───────────────────────
    print("\n[1/2]  Generating synthetic disaster events …")
    datasets = generate_all_events(
        n_earthquakes=100,   # base count; aftershock injection adds ~20–32 more
        n_fires=105,
        n_floods=105,
        n_storms=95,
    )

    # ── Task C: Write via the Dataset Writer Microservice ─────────────────────
    print("[2/2]  Writing datasets to disk …\n")
    writer = DatasetWriterService(output_dir="data")
    writer.print_summary(datasets)
    paths = writer.write_all(datasets)

    print("\nOutput files:")
    for event_type, path in paths.items():
        print(f"  [{event_type:<12}]  {path}")

    print("\n" + "=" * 60)
    print("  Step 1 complete.")
    print("  Next step: Kafka producer will stream events from data/")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
