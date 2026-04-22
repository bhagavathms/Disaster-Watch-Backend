"""
run_pipeline.py
Full Pipeline Orchestrator
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Runs the entire pipeline end-to-end with a single command.
Uses --simulate mode for Modules 2-4 (bypasses Kafka) and connects
directly to MongoDB and PostgreSQL.

Prerequisites:
    - MongoDB running on localhost:27017
    - PostgreSQL running on localhost:5432 with database 'disaster_dw'
      (create it: createdb disaster_dw)

Usage:
    python run_pipeline.py                # full pipeline, DBs must be running
    python run_pipeline.py --dry-run      # no external services needed at all
    python run_pipeline.py --skip-api     # run pipeline but don't start the API
    python run_pipeline.py --api-only     # skip pipeline, just start the API
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
PYTHON = sys.executable


# ── Helpers ──────────────────────────────────────────────────────────────────

def _banner(title: str) -> None:
    sep = "=" * 62
    print(f"\n{sep}")
    print(f"  {title}")
    print(sep)


def _run(label: str, cwd: str, args: list[str]) -> bool:
    """Run a subprocess and return True if it succeeded."""
    _banner(label)
    cmd = [PYTHON] + args
    print(f"  > {' '.join(cmd)}")
    print(f"  > cwd: {cwd}\n")

    result = subprocess.run(cmd, cwd=cwd)

    if result.returncode != 0:
        print(f"\n  [FAILED] {label} exited with code {result.returncode}")
        return False

    print(f"\n  [OK] {label} completed successfully.")
    return True


# ── Pipeline steps ───────────────────────────────────────────────────────────

def run_module_1(scale: int = 1) -> bool:
    """Module 1: Generate synthetic disaster datasets."""
    args = ["main.py"]
    if scale > 1:
        args += ["--scale", str(scale)]
    return _run(
        "Module 1 -- Synthetic Data Generation",
        PROJECT_ROOT,
        args,
    )


def run_module_2(dry_run: bool) -> bool:
    """Module 2: Kafka Producer (dry-run mode, validates data)."""
    return _run(
        "Module 2 -- Kafka Producer (dry-run)",
        os.path.join(PROJECT_ROOT, "services", "kafka-producer"),
        ["producer.py", "--dry-run"],
    )


def run_module_3() -> bool:
    """Module 3: Kafka Consumer Processor (simulate mode)."""
    return _run(
        "Module 3 -- Kafka Consumer Processor (simulate)",
        os.path.join(PROJECT_ROOT, "services", "kafka-consumer"),
        ["consumer.py", "--simulate"],
    )


def run_module_4(dry_run: bool) -> bool:
    """Module 4: MongoDB Writer (simulate mode)."""
    args = ["writer.py", "--simulate"]
    if dry_run:
        args.append("--dry-run")
    label = "Module 4 -- MongoDB Writer (simulate"
    label += ", dry-run)" if dry_run else ")"
    return _run(
        label,
        os.path.join(PROJECT_ROOT, "services", "mongodb-writer"),
        args,
    )


def run_module_5(dry_run: bool) -> bool:
    """Module 5: ETL Service (simulate mode, JSON -> PostgreSQL)."""
    args = ["etl_runner.py", "--simulate", "--create-tables"]
    if dry_run:
        args.append("--dry-run")
    label = "Module 5 -- ETL Service (simulate"
    label += ", dry-run)" if dry_run else ")"
    return _run(
        label,
        os.path.join(PROJECT_ROOT, "services", "etl-service"),
        args,
    )


def run_module_6() -> bool:
    """Module 6: Start the Query API (blocks until Ctrl+C)."""
    _banner("Module 6 -- Query API (FastAPI)")
    api_dir = os.path.join(PROJECT_ROOT, "services", "query-api")
    cmd = [PYTHON, "-m", "uvicorn", "api:app",
           "--host", "127.0.0.1", "--port", "8000", "--reload"]
    print(f"  > {' '.join(cmd)}")
    print(f"  > cwd: {api_dir}")
    print(f"\n  API docs:  http://127.0.0.1:8000/docs")
    print(f"  Health:    http://127.0.0.1:8000/health")
    print(f"\n  Press Ctrl+C to stop the server.\n")

    try:
        process = subprocess.run(cmd, cwd=api_dir)
        return process.returncode == 0
    except KeyboardInterrupt:
        print("\n  [OK] API server stopped.")
        return True


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the full Disaster Monitoring pipeline end-to-end.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py                 Full pipeline + start API
  python run_pipeline.py --dry-run       Full pipeline, no DBs needed
  python run_pipeline.py --skip-api      Run pipeline only, don't start API
  python run_pipeline.py --api-only      Skip pipeline, start API server
        """,
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run entire pipeline without MongoDB or PostgreSQL",
    )
    parser.add_argument(
        "--skip-api", action="store_true",
        help="Run Modules 1-5 but do not start the API server",
    )
    parser.add_argument(
        "--api-only", action="store_true",
        help="Skip Modules 1-5, just start the Query API server",
    )
    parser.add_argument(
        "--scale", type=int, default=1, metavar="N",
        help="Multiply base event counts by N (default: 1 → ~400 events)",
    )
    args = parser.parse_args()

    _banner("Disaster Monitoring System -- Full Pipeline")
    if args.dry_run:
        print("  Mode: DRY RUN (no external services required)")
    elif args.api_only:
        print("  Mode: API ONLY (MongoDB + PostgreSQL must be running)")
    else:
        print("  Mode: FULL (MongoDB + PostgreSQL must be running)")
    print()

    if not args.api_only:
        steps = [
            ("Module 1", lambda: run_module_1(args.scale)),
            ("Module 2", lambda: run_module_2(args.dry_run)),
            ("Module 3", lambda: run_module_3()),
            ("Module 4", lambda: run_module_4(args.dry_run)),
            ("Module 5", lambda: run_module_5(args.dry_run)),
        ]

        for name, step_fn in steps:
            if not step_fn():
                print(f"\n  Pipeline halted at {name}. Fix the error above and re-run.")
                sys.exit(1)

        _banner("Pipeline Complete -- Modules 1-5 Finished")
        print("  All data generated, processed, and loaded.\n")

        if args.skip_api:
            print("  Skipping API server (--skip-api). Done.")
            return

    # Start the API
    run_module_6()


if __name__ == "__main__":
    main()
