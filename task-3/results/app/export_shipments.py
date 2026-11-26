#!/usr/bin/env python3
import csv
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

TABLE_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def env(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and not value:
        raise ValueError(f"Environment variable {name} is required")
    return value


def log(level, event, **fields):
    print(json.dumps({
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "level": level,
        "event": event,
        **fields,
    }, ensure_ascii=False), flush=True)


def main():
    started = time.monotonic()
    table = env("EXPORT_TABLE", "shipments")
    if not TABLE_PATTERN.fullmatch(table):
        raise ValueError(f"Unsafe table name: {table}")

    export_dir = Path(env("EXPORT_DIR", "/exports"))
    export_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_file = export_dir / f"{table}_{stamp}.csv"

    config = {
        "host": env("DB_HOST", "postgres"),
        "port": int(env("DB_PORT", "5432")),
        "dbname": env("DB_NAME", "logistics"),
        "user": env("DB_USER", required=True),
        "password": env("DB_PASSWORD", required=True),
        "connect_timeout": int(env("DB_CONNECT_TIMEOUT", "10")),
    }

    query = f"SELECT * FROM {table} ORDER BY 1"
    copy_query = f"COPY ({query}) TO STDOUT WITH CSV HEADER"

    log("INFO", "export_started", table=table, output=str(output_file))
    rows = 0
    bytes_written = 0

    try:
        with psycopg2.connect(**config) as connection:
            with connection.cursor() as cursor, output_file.open("w", newline="", encoding="utf-8") as target:
                cursor.copy_expert(copy_query, target)
        bytes_written = output_file.stat().st_size
        with output_file.open(newline="", encoding="utf-8") as source:
            rows = max(sum(1 for _ in csv.reader(source)) - 1, 0)
    except Exception as exc:
        if output_file.exists():
            output_file.unlink()
        log("ERROR", "export_failed", table=table, error_type=type(exc).__name__, error=str(exc))
        return 1

    duration = round(time.monotonic() - started, 3)
    log("INFO", "export_finished", table=table, output=str(output_file), rows=rows, bytes=bytes_written, duration_seconds=duration)
    return 0


if __name__ == "__main__":
    sys.exit(main())
