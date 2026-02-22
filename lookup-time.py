#!/usr/bin/env python3
import argparse
import json
import sys
from typing import Any, Dict, Optional


def get_seconds(data: Dict[str, Any], a: str, b: str) -> Optional[float]:
    """
    Supports both formats:
    - upper-triangle: data["durations_upper"][from][to]
    - full-matrix:   data["durations_seconds"][from][to]
    Returns seconds (float) or None if missing.
    """
    a = a.strip()
    b = b.strip()
    if a == b:
        return 0.0

    # Full matrix case
    if "durations_seconds" in data:
        return data["durations_seconds"].get(a, {}).get(b)

    # Upper triangle case
    upper = data.get("durations_upper", {})
    v = upper.get(a, {}).get(b)
    if v is not None:
        return v
    v = upper.get(b, {}).get(a)
    if v is not None:
        return v

    return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Lookup travel time (seconds) between two building codes.")
    ap.add_argument("--file", required=True, help="Path to matrix JSON (upper or full)")
    ap.add_argument("from_code", help="From building code (e.g., SML)")
    ap.add_argument("to_code", help="To building code (e.g., PWG)")
    args = ap.parse_args()

    with open(args.file, "r", encoding="utf-8") as f:
        data = json.load(f)

    a = args.from_code.upper()
    b = args.to_code.upper()

    # Optional: validate codes exist if locations present
    locs = data.get("locations", {})
    if locs:
        if a not in locs:
            print(f"❌ Unknown code: {a}", file=sys.stderr)
            sys.exit(2)
        if b not in locs:
            print(f"❌ Unknown code: {b}", file=sys.stderr)
            sys.exit(2)

    seconds = get_seconds(data, a, b)
    if seconds is None:
        print(f"⚠️ No duration found for {a} <-> {b}")
        sys.exit(1)

    # Print raw seconds (easy to consume)
    print(f"{int(round(seconds / 60))} min")


if __name__ == "__main__":
    main()
