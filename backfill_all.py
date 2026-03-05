#!/usr/bin/env python3
"""Run backfill for all clients. Use --clear to wipe and rewrite."""

import json
import subprocess
import sys
import time

clear_flag = "--clear" in sys.argv

with open("clients.json") as f:
    data = json.load(f)

skip = set()  # Add client numbers here to skip, e.g. {"001"}
start = "2026-01-01"
end = "2026-03-02"

for c in data["clients"]:
    num = c["number"]
    name = c["name"]
    if num in skip:
        print(f"\n⏭ {num} {name} — skipped")
        continue

    print(f"\n{'='*60}")
    print(f"BACKFILL: {num} {name}")
    print(f"{'='*60}")

    cmd = ["python3", "backfill.py", num, start, end]
    if clear_flag:
        cmd.append("--clear")

    result = subprocess.run(
        cmd,
        input="y\n",
        text=True,
        timeout=1800,
    )

    if result.returncode != 0:
        print(f"❌ {num} {name} failed!")

    time.sleep(3)

print("\n\n" + "="*60)
print("ALL CLIENTS BACKFILL COMPLETE")
print("="*60)
