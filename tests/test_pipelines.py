"""
Tests for the Flink SQL pipelines, run on a local Flink engine (PyFlink).

The tests execute the real SQL files from flink/sql/. Only the connectors are swapped:
  * the Kafka source becomes a JSON file of generated events
  * sink tables become 'blackhole' (to check the full job plans and types line up)
  * query results are collected in memory and compared to a plain-Python calculation

Run:  pytest -v tests/
"""

import json
import random
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pyflink.common import RowKind
from pyflink.table import EnvironmentSettings, TableEnvironment

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "flink" / "sql"
sys.path.insert(0, str(ROOT / "producer"))
from producer import HIGH_VALUE_THRESHOLD, make_event  # noqa: E402

BASE = datetime(2026, 1, 1, 12, 0, 0)


# ---------------------------------------------------------------- helpers
def ddl_statements(path):
    """CREATE TABLE statements from a SQL file."""
    text = Path(path).read_text()
    return [s.strip().rstrip(";") for s in re.split(r";\s*$", text, flags=re.M)
            if "CREATE TABLE" in s]


def swap_connector(ddl, options):
    """Replace a DDL's WITH (...) clause with test connector options."""
    with_clause = ", ".join(f"'{k}' = '{v}'" for k, v in options.items())
    return re.sub(r"WITH\s*\(.*\)\s*$", f"WITH ({with_clause})", ddl, flags=re.S)


def pipeline_inserts():
    """(sink_table, select_sql) for each INSERT inside the statement set."""
    body = (SQL_DIR / "03_pipelines.sql").read_text()
    inserts = re.findall(r"INSERT INTO (\w+)\s+(SELECT.*?);", body, flags=re.S)
    return {sink: select for sink, select in inserts}


def make_env(events_file):
    env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    env.get_config().set("parallelism.default", "1")
    env.get_config().set("pipeline.auto-watermark-interval", "10 ms")
    source_ddl = ddl_statements(SQL_DIR / "01_source.sql")[0]
    env.execute_sql(swap_connector(source_ddl, {
        "connector": "filesystem",
        "path": str(events_file),
        "format": "json",
        "json.timestamp-format.standard": "ISO-8601",
        "json.ignore-parse-errors": "true",
    }))
    return env


def collect(env, select_sql):
    """Run a query; return final rows as dicts (applies upserts/retractions)."""
    table = env.sql_query(select_sql)
    names = table.get_schema().get_field_names()
    rows = []
    with table.execute().collect() as results:
        for row in results:
            values = dict(zip(names, row))
            if row.get_row_kind() in (RowKind.INSERT, RowKind.UPDATE_AFTER):
                rows.append(values)
            else:
                rows.remove(values)
    return rows


def write_events(path, events):
    with open(path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")


def generated_events(n=600, seed=7):
    """Events spread over ~4 minutes, sorted by event time (no lateness)."""
    rng = random.Random(seed)
    events = []
    for i in range(n):
        now = BASE + timedelta(milliseconds=i * 400)
        e = make_event(rng, now, i)
        e["event_time"] = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]   # in order
        events.append(e)
    return events


# ---------------------------------------------------------------- tests
@pytest.fixture(scope="module")
def run(tmp_path_factory):
    events = generated_events()
    path = tmp_path_factory.mktemp("data") / "orders.json"
    write_events(path, events)
    env = make_env(path)
    queries = pipeline_inserts()
    return events, env, queries


def test_full_job_plans_with_all_sinks(tmp_path):
    """The real statement set compiles: sink schemas and types match the queries."""
    path = tmp_path / "orders.json"
    write_events(path, generated_events(50))
    env = make_env(path)
    for ddl in ddl_statements(SQL_DIR / "02_sinks.sql"):
        env.execute_sql(swap_connector(ddl, {"connector": "blackhole"}))
    statement_set = (SQL_DIR / "03_pipelines.sql").read_text()
    statement_set = "\n".join(l for l in statement_set.splitlines() if not l.strip().startswith("--"))
    env.execute_sql(statement_set.strip().rstrip(";")).wait()


def test_tumbling_window_totals_match_source(run):
    events, env, queries = run
    rows = collect(env, queries["revenue_per_minute"])

    expected = defaultdict(lambda: [0, Decimal("0")])
    for e in events:
        minute = datetime.fromisoformat(e["event_time"]).replace(second=0, microsecond=0)
        expected[(minute, e["channel"])][0] += 1
        expected[(minute, e["channel"])][1] += Decimal(str(e["amount"]))

    assert len(rows) == len(expected)
    for r in rows:
        orders, revenue = expected[(r["window_start"], r["channel"])]
        assert r["orders"] == orders
        assert r["revenue"] == revenue.quantize(Decimal("0.01"))
        assert r["window_end"] - r["window_start"] == timedelta(minutes=1)


def test_sliding_windows_cover_five_minutes(run):
    events, env, queries = run
    rows = collect(env, queries["category_sales_sliding"])
    assert rows, "expected sliding-window output"
    assert all(r["window_end"] - r["window_start"] == timedelta(minutes=5) for r in rows)
    # Every event belongs to 5 overlapping windows, so totals are 5x the source.
    total_orders = sum(r["orders"] for r in rows)
    assert total_orders == 5 * len(events)


def test_customer_live_stats_upserts(run):
    events, env, queries = run
    rows = collect(env, queries["customer_live_stats"])

    expected = defaultdict(lambda: [0, Decimal("0")])
    for e in events:
        expected[e["customer_id"]][0] += 1
        expected[e["customer_id"]][1] += Decimal(str(e["amount"]))

    assert len(rows) == len(expected)             # exactly one row per customer
    for r in rows:
        assert r["orders"] == expected[r["customer_id"]][0]
        assert r["total_spend"] == expected[r["customer_id"]][1].quantize(Decimal("0.01"))


def test_alerts_only_high_value_orders(run):
    events, env, queries = run
    rows = collect(env, queries["order_alerts_db"])
    expected = {e["order_id"] for e in events if e["amount"] >= HIGH_VALUE_THRESHOLD}
    assert {r["order_id"] for r in rows} == expected
    assert all(r["reason"] == "HIGH_VALUE_ORDER" for r in rows)


def test_watermark_drops_very_late_event(tmp_path):
    """An event arriving after its window closed is dropped from windows,
    but still counted by the non-windowed running aggregate."""
    rng = random.Random(1)
    events = []
    for i in range(100):                                      # 12:00:00 -> 12:01:39
        e = make_event(rng, BASE + timedelta(seconds=i), i)
        e["event_time"] = (BASE + timedelta(seconds=i)).isoformat(timespec="milliseconds")
        events.append(e)
    # Plenty of later traffic so Flink emits watermarks past 12:01:00 (closing the
    # first window) before the late event is read. Watermarks are periodic, so we
    # need enough records for at least one watermark interval to pass.
    for i in range(30_000):
        e = make_event(rng, BASE, 1000 + i)
        e["event_time"] = (BASE + timedelta(seconds=100 + i / 100)).isoformat(timespec="milliseconds")
        events.append(e)
    late = dict(events[0], order_id="LATE-1", customer_id=9999,
                event_time=(BASE + timedelta(seconds=5)).isoformat(timespec="milliseconds"))
    events.append(late)   # belongs to 12:00-12:01, arrives after that window closed

    path = tmp_path / "orders.json"
    write_events(path, events)
    env = make_env(path)
    queries = pipeline_inserts()

    windows = collect(env, queries["revenue_per_minute"])
    first_minute_orders = sum(r["orders"] for r in windows if r["window_start"] == BASE)
    assert first_minute_orders == 60                          # late event NOT included

    customers = collect(env, queries["customer_live_stats"])
    assert any(r["customer_id"] == 9999 for r in customers)  # but counted here
