"""
fix_year_distribution.py
Trims fact_disaster_events so each (year, event_type) hits a
scientifically defensible target count.

Earthquake : flat ~740/year  (no secular trend in seismicity)
Flood      : very gentle rise 820 → 1200  (weak climate signal)
Storm      : moderate rise    740 → 1600  (Atlantic/Pacific basin activity)
Fire       : strongest rise   820 → 2200  (clearest climate/land-use signal)

Targets are interpolated linearly between anchor decades so there are
no abrupt step-changes in the resulting time series.
"""

import psycopg2

DSN = "host=localhost port=5432 dbname=disaster_dw user=postgres password=postgres"

# ── Per-decade anchors (start-year: target) ───────────────────────────────────
# Values represent events PER YEAR within that decade.
ANCHORS = {
    "earthquake": {1980: 740,  1990: 740,  2000: 740,  2010: 740,  2020: 740,  2025: 740},
    "flood":      {1980: 820,  1990: 880,  2000: 980,  2010: 1080, 2020: 1200, 2025: 1200},
    "storm":      {1980: 740,  1990: 880,  2000: 1080, 2010: 1320, 2020: 1600, 2025: 1600},
    "fire":       {1980: 820,  1990: 1020, 2000: 1320, 2010: 1750, 2020: 2200, 2025: 2200},
}


def interpolate_target(event_type: str, year: int) -> int:
    anchors = ANCHORS[event_type]
    keys    = sorted(anchors)
    if year <= keys[0]:
        return anchors[keys[0]]
    if year >= keys[-1]:
        return anchors[keys[-1]]
    for i in range(len(keys) - 1):
        lo, hi = keys[i], keys[i + 1]
        if lo <= year <= hi:
            t = (year - lo) / (hi - lo)
            return round(anchors[lo] + t * (anchors[hi] - anchors[lo]))
    return anchors[keys[-1]]


def main():
    conn = psycopg2.connect(DSN)
    cur  = conn.cursor()

    # Get current (year, type, count) rows
    cur.execute("""
        SELECT t.year, et.event_type_name, COUNT(*) AS cnt
        FROM fact_disaster_events f
        JOIN dim_time       t  ON f.time_id       = t.time_id
        JOIN dim_event_type et ON f.event_type_id = et.event_type_id
        GROUP BY t.year, et.event_type_name
        ORDER BY t.year, et.event_type_name
    """)
    rows = cur.fetchall()

    total_deleted = 0

    for year, etype, current_count in rows:
        target = interpolate_target(etype, year)

        if current_count <= target:
            # Already at or below target — nothing to do
            continue

        excess = current_count - target
        print(f"  {year}  {etype:<12}  current={current_count:>5}  target={target:>5}  deleting={excess:>5}")

        cur.execute("""
            DELETE FROM fact_disaster_events
            WHERE fact_id IN (
                SELECT f.fact_id
                FROM fact_disaster_events f
                JOIN dim_time       t  ON f.time_id       = t.time_id
                JOIN dim_event_type et ON f.event_type_id = et.event_type_id
                WHERE t.year              = %s
                  AND et.event_type_name  = %s
                ORDER BY random()
                OFFSET %s
            )
        """, (year, etype, target))

        total_deleted += cur.rowcount

    conn.commit()
    cur.close()
    conn.close()
    print(f"\nDone. Total rows deleted: {total_deleted:,}")


if __name__ == "__main__":
    main()
