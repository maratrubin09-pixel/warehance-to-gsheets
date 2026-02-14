#!/usr/bin/env python3
"""Run backfill for all clients that haven't been filled yet."""

import json
import subprocess
import sys
import time

with open("clients.json") as f:
    data = json.load(f)

skip = {"001"}  # Already backfilled
start = "2026-01-01"
end = "2026-02-12"

for c in data["clients"]:
    num = c["number"]
    name = c["name"]
    if num in skip:
        print(f"\n⏭ {num} {name} — already done")
        continue

    print(f"\n{'='*60}")
    print(f"BACKFILL: {num} {name}")
    print(f"{'='*60}")

    result = subprocess.run(
        ["python3", "backfill.py", num, start, end],
        input="y\n",
        text=True,
        timeout=600,
    )

    if result.returncode != 0:
        print(f"❌ {num} {name} failed!")
    
    time.sleep(3)

print("\n\n" + "="*60)
print("ALL CLIENTS BACKFILL COMPLETE")
print("="*60)
