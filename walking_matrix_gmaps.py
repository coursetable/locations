#!/usr/bin/env python3
import argparse
import json
import os
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import requests


def now_s() -> float:
    return time.time()


def chunk_indices(n: int, block_size: int) -> List[List[int]]:
    return [list(range(i, min(i + block_size, n))) for i in range(0, n, block_size)]


def total_upper_pairs(n: int) -> int:
    return (n * (n - 1)) // 2


def ensure_upper_store(store: Dict[str, Dict[str, Optional[float]]], a: str, b: str, v: Optional[float]) -> None:
    store.setdefault(a, {})
    store[a][b] = v


def load_existing(out_path: str) -> Dict[str, Any]:
    if not os.path.exists(out_path):
        return {}
    with open(out_path, "r", encoding="utf-8") as f:
        return json.load(f)


def count_done_pairs(upper: Dict[str, Dict[str, Optional[float]]]) -> int:
    return sum(len(v) for v in upper.values())


def _sleep_backoff(attempt: int, base: float = 1.2, cap: float = 30.0) -> None:
    # exponential backoff with cap
    t = min(cap, base * (2 ** attempt))
    time.sleep(t)


def gmaps_distance_matrix_request(
    api_key: str,
    origins_latlng: List[Tuple[float, float]],
    destinations_latlng: List[Tuple[float, float]],
    timeout_s: int = 60,
    max_retries: int = 6,
) -> List[List[Optional[float]]]:
    """
    Calls Google Distance Matrix API for walking duration.

    Returns: durations[origin_idx][dest_idx] in seconds (float) or None when not reachable/OK.

    Notes:
    - Uses mode=walking
    - Parses each element's duration.value (seconds) when element status == "OK"
    - Retries on 429 and 5xx with backoff
    """
    base_url = "https://maps.googleapis.com/maps/api/distancematrix/json"

    def fmt(lat: float, lng: float) -> str:
        # Google expects "lat,lng"
        return f"{lat:.7f},{lng:.7f}"

    origins_str = "|".join(fmt(lat, lng) for (lat, lng) in origins_latlng)
    dests_str = "|".join(fmt(lat, lng) for (lat, lng) in destinations_latlng)

    params = {
        "origins": origins_str,
        "destinations": dests_str,
        "mode": "walking",
        "key": api_key,
    }

    # Build final URL (GET)
    url = f"{base_url}?{urllib.parse.urlencode(params, safe='|,')}"  # keep separators

    last_err: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        try:
            r = requests.get(url, timeout=timeout_s)
            # Retry on 429 / 5xx
            if r.status_code == 429 or 500 <= r.status_code <= 599:
                _sleep_backoff(attempt)
                continue

            r.raise_for_status()
            data = r.json()

            # API-level status
            api_status = data.get("status")
            if api_status not in ("OK",):
                # Possible: OVER_QUERY_LIMIT, REQUEST_DENIED, INVALID_REQUEST, etc.
                raise RuntimeError(f"Distance Matrix API status={api_status} error={data.get('error_message')}")

            rows = data.get("rows", [])
            # rows[i].elements[j]
            out: List[List[Optional[float]]] = []
            for row in rows:
                elements = row.get("elements", [])
                row_out: List[Optional[float]] = []
                for el in elements:
                    if el.get("status") == "OK":
                        dur = el.get("duration", {}).get("value")
                        row_out.append(float(dur) if dur is not None else None)
                    else:
                        # ZERO_RESULTS / NOT_FOUND / etc.
                        row_out.append(None)
                out.append(row_out)

            # Sanity checks: shape
            if len(out) != len(origins_latlng):
                raise RuntimeError(f"Unexpected rows: got {len(out)} expected {len(origins_latlng)}")
            if out and len(out[0]) != len(destinations_latlng):
                raise RuntimeError(
                    f"Unexpected cols: got {len(out[0])} expected {len(destinations_latlng)}"
                )

            return out

        except Exception as e:
            last_err = e
            # backoff and retry unless we're out
            if attempt < max_retries:
                _sleep_backoff(attempt)
                continue
            break

    raise RuntimeError(f"Distance Matrix request failed after retries: {last_err}") from last_err


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Compute walking durations (seconds) for unique location pairs using Google Distance Matrix API."
    )
    ap.add_argument("--in", dest="in_path", required=True, help="Input locations JSON (dict keyed by code)")
    ap.add_argument("--out", dest="out_path", default="walking_upper_gmaps.json", help="Output JSON")
    ap.add_argument(
        "--block-size",
        type=int,
        default=10,
        help="Block size for matrix chunks. 10 => 100 elements/request. (Google also caps origins<=25, dest<=25.)",
    )
    ap.add_argument(
        "--max-elements",
        type=int,
        default=100,
        help="Safety cap for origins*destinations per request (default 100).",
    )
    ap.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds")
    ap.add_argument("--resume", action="store_true", help="Resume from existing --out file if present")
    ap.add_argument("--save-every", type=int, default=1, help="Save after every N block-requests")
    args = ap.parse_args()

    api_key = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("Missing GOOGLE_MAPS_API_KEY env var. Run: export GOOGLE_MAPS_API_KEY='YOUR_KEY'")

    if args.block_size <= 0:
        raise SystemExit("--block-size must be > 0")
    if args.block_size > 25:
        raise SystemExit("--block-size must be <= 25 (Google per-request origins/destinations cap)")
    if args.max_elements <= 0:
        raise SystemExit("--max-elements must be > 0")
    if args.block_size * args.block_size > args.max_elements:
        raise SystemExit(
            f"block-size^2 exceeds --max-elements ({args.block_size}^2 > {args.max_elements}). "
            f"Lower --block-size or raise --max-elements if your quota/limits allow."
        )

    with open(args.in_path, "r", encoding="utf-8") as f:
        locations: Dict[str, Any] = json.load(f)

    # Stable order = deterministic output
    codes = list(locations.keys())
    n = len(codes)

    # Your input uses {"lat":..., "lng":...}
    coords_all_latlng: List[Tuple[float, float]] = [
        (float(locations[c]["lat"]), float(locations[c]["lng"])) for c in codes
    ]

    # Output structure (upper triangle only): durations_upper[from_code][to_code] = seconds
    existing: Dict[str, Any] = load_existing(args.out_path) if args.resume else {}
    durations_upper: Dict[str, Dict[str, Optional[float]]] = existing.get("durations_upper", {}) if existing else {}

    total = total_upper_pairs(n)
    done = count_done_pairs(durations_upper)

    # Build blocks
    blocks = chunk_indices(n, args.block_size)

    # Block tasks = only upper triangle blocks (bi <= bj)
    tasks: List[Tuple[int, int]] = []
    for bi in range(len(blocks)):
        for bj in range(bi, len(blocks)):
            tasks.append((bi, bj))

    start = now_s()
    saves = 0

    print(f"Locations: {n} | Unique pairs needed: {total} | Already done: {done}")
    print(f"Blocks: {len(blocks)} (size={args.block_size}) | Block requests to run: {len(tasks)}")
    print(f"Per-request cap: max_elements={args.max_elements}")
    print("----")

    for t_idx, (bi, bj) in enumerate(tasks, start=1):
        block_i = blocks[bi]
        block_j = blocks[bj]

        # Estimate how many pairs this block could contribute
        if bi == bj:
            block_pairs = len(block_i) * (len(block_i) - 1) // 2
        else:
            block_pairs = len(block_i) * len(block_j)

        # Skip if everything already computed for this block
        if block_pairs > 0:
            missing = False
            if bi == bj:
                for a_pos in range(len(block_i)):
                    for b_pos in range(a_pos + 1, len(block_i)):
                        a = codes[block_i[a_pos]]
                        b = codes[block_i[b_pos]]
                        if a not in durations_upper or b not in durations_upper.get(a, {}):
                            missing = True
                            break
                    if missing:
                        break
            else:
                for a_idx in block_i:
                    a = codes[a_idx]
                    row = durations_upper.get(a, {})
                    for b_idx in block_j:
                        b = codes[b_idx]
                        if b not in row:
                            missing = True
                            break
                    if missing:
                        break

            if not missing:
                elapsed = now_s() - start
                pct = (done / total * 100.0) if total else 100.0
                eta = (elapsed / done) * (total - done) if done > 0 else None
                eta_str = f"{eta:,.0f}s" if eta is not None else "?"
                print(f"[{t_idx}/{len(tasks)}] block({bi},{bj}) already complete | done={done}/{total} ({pct:.1f}%) | ETA {eta_str}")
                continue

        # Safety checks for Google request sizing
        o_ct = len(block_i)
        d_ct = len(block_j) if bi != bj else len(block_i)
        if o_ct > 25 or d_ct > 25:
            raise SystemExit(f"Internal error: block sizes exceed 25 (origins={o_ct}, destinations={d_ct}).")
        if (o_ct * d_ct) > args.max_elements:
            raise SystemExit(
                f"Request would exceed --max-elements: origins*dest={o_ct*d_ct} > {args.max_elements}. "
                f"Lower --block-size or raise --max-elements."
            )

        # Compute durations for this block via Google
        if bi == bj:
            # One block: origins == destinations
            sub_global = block_i
            sub_coords_latlng = [coords_all_latlng[k] for k in sub_global]

            durations = gmaps_distance_matrix_request(
                api_key=api_key,
                origins_latlng=sub_coords_latlng,
                destinations_latlng=sub_coords_latlng,
                timeout_s=args.timeout,
            )

            # Store only a<b within block
            added = 0
            for a_pos in range(len(sub_global)):
                for b_pos in range(a_pos + 1, len(sub_global)):
                    a_code = codes[sub_global[a_pos]]
                    b_code = codes[sub_global[b_pos]]
                    if b_code in durations_upper.get(a_code, {}):
                        continue
                    v = durations[a_pos][b_pos]  # seconds or None
                    ensure_upper_store(durations_upper, a_code, b_code, v)
                    added += 1
            done += added

        else:
            # Two blocks: origins = block_i, destinations = block_j
            origins_coords = [coords_all_latlng[k] for k in block_i]
            dest_coords = [coords_all_latlng[k] for k in block_j]

            durations = gmaps_distance_matrix_request(
                api_key=api_key,
                origins_latlng=origins_coords,
                destinations_latlng=dest_coords,
                timeout_s=args.timeout,
            )

            # Store all pairs from block_i to block_j
            added = 0
            for a_pos, a_global in enumerate(block_i):
                a_code = codes[a_global]
                row = durations_upper.get(a_code, {})
                for b_pos, b_global in enumerate(block_j):
                    b_code = codes[b_global]
                    if b_code in row:
                        continue
                    v = durations[a_pos][b_pos]  # seconds or None
                    ensure_upper_store(durations_upper, a_code, b_code, v)
                    added += 1
            done += added

        # Progress + ETA
        elapsed = now_s() - start
        pct = (done / total * 100.0) if total else 100.0
        eta = (elapsed / done) * (total - done) if done > 0 else None
        eta_str = f"{eta:,.0f}s" if eta is not None else "?"
        print(f"[{t_idx}/{len(tasks)}] block({bi},{bj}) | +{added} | done={done}/{total} ({pct:.1f}%) | ETA {eta_str}")

        # Periodic save
        if (t_idx % args.save_every) == 0:
            result = {
                "meta": {
                    "source": "google_distance_matrix",
                    "endpoint": "https://maps.googleapis.com/maps/api/distancematrix/json",
                    "mode": "walking",
                    "units": {"duration": "seconds"},
                    "location_count": n,
                    "stored_pairs": done,
                    "note": "Upper triangle only: durations_upper[from][to] where to is later in input order; diagonal omitted.",
                },
                "locations": {
                    code: {
                        "name": locations[code].get("name"),
                        "lat": float(locations[code].get("lat")),
                        "lng": float(locations[code].get("lng")),
                    }
                    for code in codes
                },
                "durations_upper": durations_upper,
            }
            with open(args.out_path, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            saves += 1

    # Final save
    result = {
        "meta": {
            "source": "google_distance_matrix",
            "endpoint": "https://maps.googleapis.com/maps/api/distancematrix/json",
            "mode": "walking",
            "units": {"duration": "seconds"},
            "location_count": n,
            "stored_pairs": done,
            "note": "Upper triangle only: durations_upper[from][to] where to is later in input order; diagonal omitted.",
        },
        "locations": {
            code: {
                "name": locations[code].get("name"),
                "lat": float(locations[code].get("lat")),
                "lng": float(locations[code].get("lng")),
            }
            for code in codes
        },
        "durations_upper": durations_upper,
    }
    with open(args.out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    elapsed = now_s() - start
    print("----")
    print(f"✅ Wrote {args.out_path}")
    print(f"Pairs stored: {done}/{total} | Total time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
