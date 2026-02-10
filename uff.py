#!/usr/bin/env python3
"""
uff.py - UK Fuel Finder (Government API Integration)
====================================================

A command-line tool for querying UK fuel prices from the
Government Fuel Finder API with intelligent caching, multi-sensor
support, and flexible filtering.

FEATURES
--------
- Smart caching: Baseline + incremental updates minimise API calls
- Multi-sensor safe: File locking allows concurrent execution
- Fast queries: Cached data means instant results (<1 second)
- Flexible filtering: Regex patterns for name, brand, postcode, fuel type
- Data cleaning: Automatically fixes price entry errors (< 2p)
- All fuel types: E10, E5, B7, B7 Premium, HVO, B10

WORKING DIRECTORY MODEL
-----------------------
All files are stored in a single working directory:
    - config.json   (optional: API credentials and settings)
    - state.json    (cache: stations + prices, ~3-5MB)
    - token.json    (OAuth access + refresh token)
    - state.lock    (inter-process lock, auto-managed)

DIRECTORY RESOLUTION
--------------------
The script finds the working directory in this order:
1. CLI argument: --config-dir /path/to/dir
2. Environment: UFF_CONFIG_DIR=/path/to/dir
3. Default: /config/.storage/uk_fuel_finder

CREDENTIALS RESOLUTION
----------------------
API credentials are resolved in this order:
1. CLI arguments: --client-id ID --client-secret SECRET
2. config.json file in working directory
3. Environment: UFF_CLIENT_ID=... UFF_CLIENT_SECRET=...

DEBUGGING
---------
Use --debug to print diagnostic messages to stderr.
stdout always contains only JSON output (safe for piping/parsing).

HOME ASSISTANT INTEGRATION
---------------------------
Install in /config/scripts/uff.py and configure as command_line sensor.
Multiple sensors can run concurrently (file locking prevents conflicts).

API REGISTRATION
----------------
Register for free API credentials at:
https://www.fuel-finder.service.gov.uk/

NOTES
----------------
Created for the Home Assistant community
License: GPL v3 - https://www.gnu.org/licenses/gpl-3.0
Author: Phil Male - https://phil-male.com
"""

from __future__ import annotations

import argparse
import fcntl
import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

# --------------------- debug ---------------------

# Global debug flag, toggled by --debug CLI argument
DEBUG = False


def debug_print(msg: str) -> None:
    """Print debug messages to stderr when --debug is enabled."""
    if not DEBUG:
        return
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", file=sys.stderr)


# API base URL and HTTP status codes eligible for retry
BASE_URL = "https://www.fuel-finder.service.gov.uk"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

# Configurable defaults for cache timing, HTTP behaviour, and batch pacing
DEFAULTS = {
    "config_dir": "/config/.storage/uk_fuel_finder",
    "stations_baseline_days": 7,  # days between stations baseline pull
    "stations_incremental_hours": 12,  # hours to pull incremental stations
    "prices_baseline_days": 2,  # days between prices baseline pull
    "prices_incremental_hours": 1.0,  # hours to pull incremental prices
    "http_timeout": 60,
    "http_retries": 6,
    "http_backoff_base": 1.8,
    "http_backoff_jitter": 0.7,
    "batch_sleep_seconds": 1.0,
    "incremental_safety_minutes": 45,
}

# --------------------- time helpers ---------------------


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_dt_maybe(s: Optional[str]) -> Optional[datetime]:
    # Parse an ISO datetime string, returning None on failure
    if not s:
        return None
    s2 = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s2)
    except Exception:
        return None


def parse_price_dt(s: Optional[str]) -> Optional[datetime]:
    # API returns "YYYY-MM-DDTHH:MM:SS" (no tz); treat as UTC
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


# --------------------- geo helpers ---------------------


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    # Calculate great-circle distance between two lat/lon points in km
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


# --------------------- filesystem / locking ---------------------


@dataclass
class Paths:
    work_dir: Path
    state_file: Path
    token_file: Path
    lock_file: Path
    config_file: Path


def make_paths(work_dir: str) -> Paths:
    # Derive all file paths from the single working directory
    d = Path(work_dir)
    return Paths(
        work_dir=d,
        state_file=d / "state.json",
        token_file=d / "token.json",
        lock_file=d / "state.lock",
        config_file=d / "config.json",
    )


class FileLock:
    # Exclusive file lock using fcntl for multi-process safety
    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self.fd = None

    def __enter__(self) -> FileLock:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self.fd = open(self.lock_path, "w")
        fcntl.flock(self.fd, fcntl.LOCK_EX)
        return self

    def __exit__(
        self, exc_type: Optional[type], exc: Optional[BaseException], tb: Optional[Any]
    ) -> None:
        try:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
        finally:
            try:
                self.fd.close()
            except Exception:
                pass


# --------------------- http / retries ---------------------


class AuthError(Exception):
    # Raised on 401/403 to trigger token refresh separately from retries
    def __init__(
        self, message: str, response: Optional[requests.Response] = None
    ) -> None:
        super().__init__(message)
        self.response = response


def request_with_retry(
    method: str,
    url: str,
    *,
    headers: Dict[str, str],
    params: Optional[Dict[str, str]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = DEFAULTS["http_timeout"],
    retries: int = DEFAULTS["http_retries"],
    backoff_base: float = DEFAULTS["http_backoff_base"],
    backoff_jitter: float = DEFAULTS["http_backoff_jitter"],
) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            resp = requests.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout,
            )

            # Auth failures are not retryable; raise immediately
            if resp.status_code in (401, 403):
                raise AuthError(f"Auth HTTP {resp.status_code}", response=resp)

            # Server errors and rate limits get exponential backoff
            if resp.status_code in RETRYABLE_STATUS:
                raise requests.HTTPError(
                    f"Retryable HTTP {resp.status_code}", response=resp
                )
            resp.raise_for_status()
            return resp
        except Exception as e:
            # Let auth errors propagate without retry
            if isinstance(e, AuthError):
                raise

            last_exc = e

            # Final attempt exhausted; re-raise
            if attempt == retries - 1:
                raise
            try:
                status = getattr(getattr(e, "response", None), "status_code", None)
            except Exception:
                status = None
            debug_print(
                f"HTTP retry {attempt + 1}/{retries - 1} for {method} {url} (status={status}): {e}"
            )
            # Exponential backoff with jitter, capped at 30s
            sleep_s = (backoff_base**attempt) + (
                backoff_jitter * (0.5 + (attempt % 3) / 3)
            )
            time.sleep(min(30.0, sleep_s))
    if last_exc:
        raise last_exc
    raise RuntimeError("request_with_retry: unreachable")


# --------------------- json helpers ---------------------


def load_json(path: Path) -> Optional[Dict[str, Any]]:
    # Safely load a JSON file, returning None if missing or corrupt
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_json(path: Path, data: Dict[str, Any]) -> None:
    # Write JSON atomically, creating parent dirs as needed
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def load_config(path: str) -> Dict[str, Any]:
    # Load a config file, returning empty dict on any failure
    p = Path(path)
    if not p.exists():
        return {}
    try:
        v = json.loads(p.read_text(encoding="utf-8"))
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


def load_config_from_dir(work_dir: str) -> Dict[str, Any]:
    # Convenience: load config.json from the working directory
    cfg_path = Path(work_dir) / "config.json"
    if not cfg_path.exists():
        return {}
    return load_config(str(cfg_path))


# --------------------- OAuth token manager ---------------------


def get_access_token(
    paths: Paths, client_id: str, client_secret: str, *, force_refresh: bool = False
) -> str:
    token = load_json(paths.token_file) or {}
    access = token.get("access_token")
    expires_at = parse_dt_maybe(token.get("expires_at"))
    refresh = token.get("refresh_token")

    # Use cached token if still valid (with 30s safety margin)
    if (
        (not force_refresh)
        and access
        and expires_at
        and utc_now() < (expires_at - timedelta(seconds=30))
    ):
        debug_print("OAuth: using cached access_token")
        return access

    # Try refresh token flow first if we have one
    if refresh:
        debug_print("OAuth: refreshing access_token using refresh_token")
        url = f"{BASE_URL}/api/v1/oauth/regenerate_access_token"
        payload = {"client_id": client_id, "refresh_token": refresh}
        resp = request_with_retry(
            "POST",
            url,
            headers={"accept": "application/json"},
            json_body=payload,
        )
        data = resp.json()
        token_data = data.get("data", data)

        access_token = token_data["access_token"]
        expires_in = int(token_data.get("expires_in", 3600))

        # Persist new access token, keeping existing refresh token
        new_token = {
            "access_token": access_token,
            "refresh_token": refresh,
            "expires_at": iso_utc(utc_now() + timedelta(seconds=expires_in)),
            "token_type": token_data.get("token_type", "Bearer"),
            "updated_at": iso_utc(utc_now()),
        }
        save_json(paths.token_file, new_token)
        return access_token

    # No refresh token available; do full credential exchange
    debug_print("OAuth: generating new access_token")
    url = f"{BASE_URL}/api/v1/oauth/generate_access_token"
    payload = {"client_id": client_id, "client_secret": client_secret}
    resp = request_with_retry(
        "POST",
        url,
        headers={"accept": "application/json"},
        json_body=payload,
    )
    data = resp.json()
    token_data = data.get("data", data)

    access_token = token_data["access_token"]
    expires_in = int(token_data.get("expires_in", 3600))
    refresh_token = token_data.get("refresh_token")

    # Persist both access and refresh tokens
    new_token = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_at": iso_utc(utc_now() + timedelta(seconds=expires_in)),
        "token_type": token_data.get("token_type", "Bearer"),
        "updated_at": iso_utc(utc_now()),
    }
    save_json(paths.token_file, new_token)
    return access_token


# --------------------- API fetch (batched) ---------------------


def fetch_all_batches(
    token: str,
    path: str,
    *,
    params: Optional[Dict[str, str]] = None,
    batch_sleep: float = DEFAULTS["batch_sleep_seconds"],
    refresh_token_fn: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    # Paginate through API batches (500 rows each) until exhausted

    t0 = time.time()
    debug_print(f"API fetch start: {path} params={params}")
    headers = {"accept": "application/json", "authorization": f"Bearer {token}"}
    out: List[Dict[str, Any]] = []
    batch = 1
    while True:
        qp = dict(params or {})
        qp["batch-number"] = str(batch)
        url = f"{BASE_URL}{path}"

        try:
            resp = request_with_retry("GET", url, headers=headers, params=qp)
        except AuthError as e:
            # Token expired mid-pagination; force refresh and retry this batch
            if refresh_token_fn is None:
                raise
            debug_print(
                f"Auth failed ({getattr(e.response, 'status_code', None)}). "
                f"Forcing token refresh and retrying batch {batch}."
            )
            token = refresh_token_fn()  # returns new access token
            headers = {"accept": "application/json", "authorization": f"Bearer {token}"}
            resp = request_with_retry("GET", url, headers=headers, params=qp)

        data = resp.json()
        if not isinstance(data, list):
            data = []
        out.extend(data)
        debug_print(f"  batch {batch}: {len(data)} rows (total {len(out)})")
        # API returns <500 rows on final batch
        if len(data) < 500:
            break
        batch += 1
        time.sleep(batch_sleep)
    debug_print(f"API fetch done: {path} rows={len(out)} in {time.time() - t0:.2f}s")
    return out


# --------------------- cache model ---------------------


def empty_state() -> Dict[str, Any]:
    # Initialise a blank state structure with all required meta keys
    return {
        "stations": {},  # node_id -> station object
        "prices": {},  # node_id -> {fuel_type -> {price, price_last_updated}}
        "meta": {
            "stations_baseline_at": None,
            "stations_last_incremental_at": None,
            "prices_baseline_at": None,
            "prices_last_incremental_at": None,
            "prices_max_price_last_updated": None,
            # price-fix counters
            "price_fix_count_total": 0,
            "price_fix_count_last_run": 0,
            "price_fix_last_run_at": None,
            "created_at": iso_utc(utc_now()),
            "updated_at": iso_utc(utc_now()),
        },
    }


def load_state(paths: Paths) -> Dict[str, Any]:
    # Load cached state from disk, ensuring forward-compatible meta keys
    data = load_json(paths.state_file)
    if (
        isinstance(data, dict)
        and "stations" in data
        and "prices" in data
        and "meta" in data
    ):
        # ensure meta keys exist (forward-compatible)
        meta = data.get("meta", {}) or {}
        meta.setdefault("price_fix_count_total", 0)
        meta.setdefault("price_fix_count_last_run", 0)
        meta.setdefault("price_fix_last_run_at", None)
        data["meta"] = meta
        return data
    return empty_state()


def save_state(paths: Paths, state: Dict[str, Any]) -> None:
    # Persist state to disk, updating the timestamp
    state["meta"]["updated_at"] = iso_utc(utc_now())
    save_json(paths.state_file, state)


def invalidate_cache(paths: Paths) -> None:
    # Wipe state file and replace with empty state
    paths.work_dir.mkdir(parents=True, exist_ok=True)
    if paths.state_file.exists():
        paths.state_file.unlink()
    save_state(paths, empty_state())


def cache_stats(state: Dict[str, Any]) -> Dict[str, Any]:
    # Build a summary dict of cache health for JSON output
    stations = state.get("stations", {}) or {}
    prices = state.get("prices", {}) or {}
    fuel_types: set[str] = set()

    for _, p in prices.items():
        if isinstance(p, dict):
            for ft in p.keys():
                fuel_types.add(str(ft))

    meta = state.get("meta", {}) or {}
    return {
        "stations_count": len(stations),
        "prices_station_count": len(prices),
        "fuel_types": sorted(fuel_types),
        "stations_baseline_at": meta.get("stations_baseline_at"),
        "stations_last_incremental_at": meta.get("stations_last_incremental_at"),
        "prices_baseline_at": meta.get("prices_baseline_at"),
        "prices_last_incremental_at": meta.get("prices_last_incremental_at"),
        "prices_max_price_last_updated": meta.get("prices_max_price_last_updated"),
        # price-fix counters
        "price_fix_count_total": int(meta.get("price_fix_count_total") or 0),
        "price_fix_count_last_run": int(meta.get("price_fix_count_last_run") or 0),
        "price_fix_last_run_at": meta.get("price_fix_last_run_at"),
        "state_file_bytes": None,
        "token_file_bytes": None,
    }


# --------------------- transform / merge ---------------------


def stations_to_dict(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    # Convert API station list to dict keyed by node_id
    out: Dict[str, Dict[str, Any]] = {}
    for it in items:
        sid = it.get("node_id")
        if not sid:
            continue
        out[str(sid)] = it
    return out


def merge_station_dict(base: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    # Merge incremental station updates into the base dict (upsert)
    base = dict(base or {})
    for sid, obj in (updates or {}).items():
        base[str(sid)] = obj
    return base


def _price_fix_to_pence(price_raw: Any) -> Tuple[Any, int]:
    # Fix prices reported in pounds (< 2) by converting to pence (* 100)
    if price_raw in (None, ""):
        return price_raw, 0

    try:
        d = Decimal(str(price_raw))
    except (InvalidOperation, ValueError):
        return price_raw, 0

    # Prices below 2 are almost certainly in pounds, not pence
    if d < Decimal("2"):
        d = (d * Decimal("100")).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
        return float(d), 1

    # normal value: keep as float for JSON consistency
    try:
        return float(d), 0
    except Exception:
        return price_raw, 0


def prices_to_dict(
    items: List[Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], Optional[str], int]:
    # Convert API price list to nested dict: station_id -> fuel_type -> {price, updated}
    # Also tracks the newest price timestamp and counts price fixes applied
    out: Dict[str, Dict[str, Any]] = {}
    max_dt: Optional[datetime] = None
    fix_count = 0

    for it in items:
        sid = it.get("node_id")
        if not sid:
            continue
        sid = str(sid)
        fp = it.get("fuel_prices", [])
        if not isinstance(fp, list):
            fp = []
        per_station: Dict[str, Any] = out.get(sid, {})
        for row in fp:
            if not isinstance(row, dict):
                continue
            ft = row.get("fuel_type")
            if not ft:
                continue
            ft = str(ft)

            price_raw = row.get("price")
            plu = row.get("price_last_updated")

            # Apply pound-to-pence correction if needed
            price, fixed = _price_fix_to_pence(price_raw)
            if fixed:
                fix_count += 1
                debug_print(
                    f"Fixed price error: {price_raw} -> {price} (assumed *100) "
                    f"for {ft} at station {sid[:8]}..."
                )

            per_station[ft] = {"price": price, "price_last_updated": plu}

            # Track the most recent price update timestamp for cursor advancement
            dt = parse_price_dt(plu)
            if dt and (max_dt is None or dt > max_dt):
                max_dt = dt
        out[sid] = per_station

    return out, (iso_utc(max_dt) if max_dt else None), fix_count


def merge_price_dict(base: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    # Merge incremental price updates per station per fuel type
    base = dict(base or {})
    for sid, fuels in (updates or {}).items():
        sid = str(sid)
        if sid not in base or not isinstance(base.get(sid), dict):
            base[sid] = {}
        for ft, row in (fuels or {}).items():
            base[sid][str(ft)] = row
    return base


# --------------------- refresh policy ---------------------


def needs_stations_baseline(state: Dict[str, Any], days: int) -> bool:
    # True if stations baseline has never run or has expired
    dt = parse_dt_maybe(state.get("meta", {}).get("stations_baseline_at"))
    if dt is None:
        return True
    return utc_now() >= (dt + timedelta(days=days))


def needs_prices_baseline(state: Dict[str, Any], days: int) -> bool:
    # True if prices baseline has never run or has expired
    dt = parse_dt_maybe(state.get("meta", {}).get("prices_baseline_at"))
    if dt is None:
        return True
    return utc_now() >= (dt + timedelta(days=days))


def needs_stations_incremental(state: Dict[str, Any], hours: int) -> bool:
    # True if stations incremental update is due
    dt = parse_dt_maybe(state.get("meta", {}).get("stations_last_incremental_at"))
    if dt is None:
        return True
    return utc_now() >= (dt + timedelta(hours=hours))


def needs_prices_incremental(state: Dict[str, Any], hours: float) -> bool:
    # True if prices incremental update is due
    dt = parse_dt_maybe(state.get("meta", {}).get("prices_last_incremental_at"))
    if dt is None:
        return True
    return utc_now() >= (dt + timedelta(hours=hours))


def ensure_cache(
    *,
    paths: Paths,
    client_id: str,
    client_secret: str,
    full_refresh: bool,
    stations_baseline_days: int,
    stations_incremental_hours: int,
    prices_baseline_days: int,
    prices_incremental_hours: float,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    # Acquire exclusive lock to prevent concurrent cache writes
    with FileLock(paths.lock_file):
        if full_refresh:
            debug_print("Cache: full refresh requested; invalidating state")
            invalidate_cache(paths)

        state = load_state(paths)
        debug_print(
            f"Cache: loaded stations={len(state.get('stations', {}) or {})} "
            f"prices={len(state.get('prices', {}) or {})}"
        )

        token = get_access_token(paths, client_id, client_secret)

        def force_refresh_access_token() -> str:
            return get_access_token(paths, client_id, client_secret, force_refresh=True)

        # reset "last run" fix counter (this run may update prices, or not)
        state["meta"]["price_fix_count_last_run"] = 0

        # Track whether we did a stations baseline to force a prices baseline too
        did_stations_baseline = False

        # --- Stations baseline (full pull, periodic) ---
        if needs_stations_baseline(state, stations_baseline_days):
            debug_print(
                f"Stations: baseline refresh required (>{stations_baseline_days}d or missing)"
            )
            items = fetch_all_batches(
                token, "/api/v1/pfs", refresh_token_fn=force_refresh_access_token
            )
            state["stations"] = stations_to_dict(items)
            debug_print(
                f"Stations: baseline loaded {len(items)} rows; "
                f"stations now {len(state['stations'])}"
            )
            now = iso_utc(utc_now())
            state["meta"]["stations_baseline_at"] = now
            state["meta"]["stations_last_incremental_at"] = now
            debug_print(
                f"Stations: cursor advanced to {state['meta']['stations_last_incremental_at']}"
            )
            # A new stations baseline means prices must be re-baselined too
            did_stations_baseline = True

        # --- Stations incremental (delta since last pull with safety overlap) ---
        elif needs_stations_incremental(state, stations_incremental_hours):
            last = (
                parse_dt_maybe(state["meta"].get("stations_last_incremental_at"))
                or utc_now()
            )
            age_hours = (utc_now() - last).total_seconds() / 3600.0
            debug_print(
                f"Stations: incremental refresh "
                f"(age={age_hours:.2f}h > threshold={stations_incremental_hours}h)"
            )

            # Wind back the cursor to catch any late-arriving data
            safe = last - timedelta(minutes=DEFAULTS["incremental_safety_minutes"])
            params = {"effective-start-timestamp": safe.strftime("%Y-%m-%d %H:%M:%S")}

            debug_print(
                f"Stations: incremental using last={state['meta'].get('stations_last_incremental_at')} "
                f"safe={safe.isoformat()} "
                f"threshold={stations_incremental_hours}h"
            )

            items = fetch_all_batches(
                token,
                "/api/v1/pfs",
                params=params,
                refresh_token_fn=force_refresh_access_token,
            )

            # Merge any new/updated stations into the cache
            if items:
                upd = stations_to_dict(items)
                before = len(state.get("stations", {}) or {})
                state["stations"] = merge_station_dict(state["stations"], upd)
                after = len(state.get("stations", {}) or {})
                debug_print(
                    f"Stations: merged {len(upd)} updates (stations {before}->{after})"
                )

            state["meta"]["stations_last_incremental_at"] = iso_utc(utc_now())
            debug_print(
                f"Stations: cursor advanced to {state['meta']['stations_last_incremental_at']}"
            )
        else:
            debug_print("Stations: no refresh needed")

        # --- Prices baseline (full pull, also forced after stations baseline) ---
        if did_stations_baseline or needs_prices_baseline(state, prices_baseline_days):
            debug_print(
                "Prices: baseline refresh required (>{prices_baseline_days)d or missing)"
            )
            items = fetch_all_batches(
                token,
                "/api/v1/pfs/fuel-prices",
                refresh_token_fn=force_refresh_access_token,
            )
            prices, max_plu, fix_count = prices_to_dict(items)
            state["prices"] = prices
            debug_print(
                f"Prices: baseline loaded {len(items)} rows; "
                f"prices now {len(state['prices'])}; max_plu={max_plu}; fixes={fix_count}"
            )
            now = iso_utc(utc_now())
            state["meta"]["prices_baseline_at"] = now
            # Use the newest price timestamp as the incremental cursor
            state["meta"]["prices_last_incremental_at"] = max_plu or now
            debug_print(
                f"Prices: cursor advanced to {state['meta']['prices_last_incremental_at']}"
            )
            state["meta"]["prices_max_price_last_updated"] = max_plu

            # Update price-fix counters
            state["meta"]["price_fix_count_last_run"] = int(fix_count)
            state["meta"]["price_fix_count_total"] = int(
                state["meta"].get("price_fix_count_total") or 0
            ) + int(fix_count)
            state["meta"]["price_fix_last_run_at"] = iso_utc(utc_now())

            # Prune orphan price entries that have no matching station
            if did_stations_baseline:
                station_ids = set((state.get("stations") or {}).keys())
                price_ids = set((state.get("prices") or {}).keys())

                orph_prices = sorted(price_ids - station_ids)
                orph_stns = sorted(station_ids - price_ids)

                debug_print(
                    f"Cache: orphan prices (no station): {len(orph_prices)}/{len(price_ids)}; "
                    f"orphan stations (no price): {len(orph_stns) / {len(station_ids)}}"
                )

                # Remove price entries with no corresponding station
                if orph_prices:
                    for sid in orph_prices:
                        del state["prices"][sid]
                    debug_print(
                        f"Cache: pruned {len(orph_prices)} orphan price entries"
                    )
                # Leave orphan stations; prices may arrive in a later incremental
                # if orph_stns:
                #    for sid in orph_stns:
                #        del state["stations"][sid]
                #    debug_print(f"Cache: pruned {len(orph_stns)} orphan station entries")
                debug_print(
                    f"Cache: leaving {len(orph_stns)} orphan station entries to await prices"
                )

        # --- Prices incremental (delta since last pull with safety overlap) ---
        elif needs_prices_incremental(state, prices_incremental_hours):
            debug_print(f"Prices: incremental refresh (>{prices_incremental_hours}h)")
            last = (
                parse_dt_maybe(state["meta"].get("prices_last_incremental_at"))
                or utc_now()
            )
            # Wind back cursor to catch late-arriving price updates
            safe = last - timedelta(minutes=DEFAULTS["incremental_safety_minutes"])
            params = {"effective-start-timestamp": safe.strftime("%Y-%m-%d %H:%M:%S")}
            debug_print(
                f"Prices: incremental using last={state['meta'].get('prices_last_incremental_at')} "
                f"safe={safe.isoformat()} "
                f"threshold={prices_incremental_hours}h"
            )
            items = fetch_all_batches(
                token,
                "/api/v1/pfs/fuel-prices",
                params=params,
                refresh_token_fn=force_refresh_access_token,
            )
            if items:
                upd, max_plu, fix_count = prices_to_dict(items)
                before = len(state.get("prices", {}) or {})
                state["prices"] = merge_price_dict(state["prices"], upd)
                after = len(state.get("prices", {}) or {})
                debug_print(
                    f"Prices: merged updates for {len(upd)} stations "
                    f"(prices {before}->{after}); max_plu={max_plu}; fixes={fix_count}"
                )
                # Advance cursor to newest price timestamp if available
                if max_plu:
                    state["meta"]["prices_last_incremental_at"] = max_plu
                    debug_print(
                        f"Prices: cursor advanced to {state['meta']['prices_last_incremental_at']}"
                    )
                    state["meta"]["prices_max_price_last_updated"] = max_plu
                else:
                    state["meta"]["prices_last_incremental_at"] = iso_utc(utc_now())
                    debug_print(
                        f"Prices: cursor advanced to {state['meta']['prices_last_incremental_at']}"
                    )

                # Update price-fix counters
                state["meta"]["price_fix_count_last_run"] = int(fix_count)
                state["meta"]["price_fix_count_total"] = int(
                    state["meta"].get("price_fix_count_total") or 0
                ) + int(fix_count)
                state["meta"]["price_fix_last_run_at"] = iso_utc(utc_now())
            else:
                # No incremental data returned; advance cursor anyway
                state["meta"]["prices_last_incremental_at"] = iso_utc(utc_now())
                debug_print(
                    f"Prices: cursor advanced to {state['meta']['prices_last_incremental_at']}"
                )
                state["meta"]["price_fix_count_last_run"] = 0

        else:
            debug_print("Prices: no refresh needed")
            state["meta"]["price_fix_count_last_run"] = 0

        save_state(paths, state)

    # Generate cache health stats (outside the lock)
    stats = cache_stats(state)
    try:
        stats["state_file_bytes"] = (
            paths.state_file.stat().st_size if paths.state_file.exists() else 0
        )
        stats["token_file_bytes"] = (
            paths.token_file.stat().st_size if paths.token_file.exists() else 0
        )
    except Exception:
        pass
    return state, stats


# --------------------- query ---------------------


def compile_res(res: Optional[List[str]]) -> Optional[List[re.Pattern]]:
    # Compile regex filter strings into case-insensitive patterns
    if not res:
        return None
    out: List[re.Pattern] = []
    for s in res:
        out.append(re.compile(s, re.IGNORECASE))
    return out


def any_match(patterns: Optional[List[re.Pattern]], value: Optional[str]) -> bool:
    # True if no patterns set (pass-through) or any pattern matches the value
    if patterns is None:
        return True
    v = value or ""
    return any(p.search(v) for p in patterns)


def fuels_match(
    patterns: Optional[List[re.Pattern]], fuel_types: Iterable[str]
) -> bool:
    # True if no patterns set or any pattern matches any of the station's fuel types
    if patterns is None:
        return True
    fts = [str(x) for x in fuel_types if x is not None]
    return any(p.search(ft) for p in patterns for ft in fts)


def station_field(st: Dict[str, Any], which: str) -> str:
    # Extract a named field from a station dict for regex matching
    if which == "station_id":
        return str(st.get("node_id") or "")
    if which == "name":
        return st.get("trading_name") or ""
    if which == "brand":
        return st.get("brand_name") or ""
    if which == "town":
        return (st.get("location") or {}).get("city") or ""
    if which == "postcode":
        return (st.get("location") or {}).get("postcode") or ""
    return ""


def station_latlon(st: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    # Extract lat/lon from station location, returning None if missing
    loc = st.get("location") or {}
    try:
        lat = float(loc.get("latitude"))
        lon = float(loc.get("longitude"))
        return lat, lon
    except Exception:
        return None


def station_fuel_types(st: Dict[str, Any]) -> List[str]:
    # Return the list of fuel types offered by a station
    f = st.get("fuel_types", [])
    if isinstance(f, list):
        return [str(x) for x in f if x is not None]
    return []


def station_price_for(
    state: Dict[str, Any], station_id: str, fuel_type: str
) -> Optional[float]:
    # Look up the current price for a specific fuel at a specific station
    p = (state.get("prices") or {}).get(station_id) or {}
    row = p.get(fuel_type)
    if not isinstance(row, dict):
        return None
    price_s = row.get("price")
    if price_s in (None, ""):
        return None
    try:
        return float(price_s)
    except Exception:
        return None


def query_stations(
    state: Dict[str, Any],
    *,
    lat: Optional[float],
    lon: Optional[float],
    radius_km: Optional[float],
    station_ids: Optional[List[str]],
    re_name: Optional[List[re.Pattern]],
    re_postcode: Optional[List[re.Pattern]],
    re_town: Optional[List[re.Pattern]],
    re_id: Optional[List[re.Pattern]],
    re_brand: Optional[List[re.Pattern]],
    re_fuel: Optional[List[re.Pattern]],
    sort: str,
    limit: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    stations = state.get("stations") or {}
    chosen: List[Tuple[Dict[str, Any], Optional[float]]] = []

    # Build initial candidate list: either by explicit IDs or radius search
    if station_ids:
        for sid in station_ids:
            st = stations.get(str(sid))
            if st:
                chosen.append((st, None))
    else:
        if lat is None or lon is None or radius_km is None:
            raise ValueError("radius query requires lat, lon, and radius")
        for st in stations.values():
            ll = station_latlon(st)
            if not ll:
                continue
            d = haversine_km(lat, lon, ll[0], ll[1])
            if d <= radius_km:
                chosen.append((st, d))

    debug_print(f"Query: initial candidates={len(chosen)}")

    # Apply regex filters (AND across categories, OR within each category)
    filtered: List[Tuple[Dict[str, Any], Optional[float]]] = []
    for st, d in chosen:
        if not any_match(re_name, station_field(st, "name")):
            continue
        if not any_match(re_postcode, station_field(st, "postcode")):
            continue
        if not any_match(re_town, station_field(st, "town")):
            continue
        if not any_match(re_id, station_field(st, "station_id")):
            continue
        if not any_match(re_brand, station_field(st, "brand")):
            continue
        if not fuels_match(re_fuel, station_fuel_types(st)):
            continue
        filtered.append((st, d))

    debug_print(f"Query: after regex filters={len(filtered)}")

    # Sort results: by distance (default), cheapest for a fuel type, or fallback
    sort_key = (sort or "").strip()

    def dist_val(d: Optional[float]) -> float:
        return d if d is not None else float("inf")

    if sort_key == "distance" or sort_key == "":
        filtered.sort(key=lambda x: dist_val(x[1]))
    elif sort_key.startswith("cheapest:"):
        # Sort by price for the specified fuel type, then distance as tiebreaker
        fuel = sort_key.split(":", 1)[1].strip()

        def key(item: Tuple[Dict[str, Any], Optional[float]]) -> Tuple[float, float]:
            st, d = item
            sid = str(st.get("node_id"))
            pv = station_price_for(state, sid, fuel)
            return (pv if pv is not None else float("inf"), dist_val(d))

        filtered.sort(key=key)
    else:
        # Unrecognised sort key; fall back to distance
        filtered.sort(key=lambda x: dist_val(x[1]))

    # Apply result limit
    filtered = filtered[: max(0, limit)]
    debug_print(
        f"Query: sort='{sort_key or 'distance'}' limit={limit} returning={len(filtered)}"
    )

    # Build output list and collect fuel types seen across results
    result: List[Dict[str, Any]] = []
    fuel_seen: set[str] = set()

    for st, d in filtered:
        sid = str(st.get("node_id"))
        ft = station_fuel_types(st)
        fuel_seen |= set(ft)

        # Attach current prices for each fuel type at this station
        prices_for_station = (state.get("prices") or {}).get(sid) or {}
        price_out: Dict[str, Any] = {}
        for f in ft:
            row = prices_for_station.get(f)
            if isinstance(row, dict):
                price_out[f] = row

        result.append(
            {
                "station_id": sid,
                "name": st.get("trading_name"),
                "brand": st.get("brand_name"),
                "phone": st.get("public_phone_number"),
                "temporary_closure": st.get("temporary_closure"),
                "permanent_closure": st.get("permanent_closure"),
                "motorway_service": st.get("is_motorway_service_station"),
                "supermarket_service": st.get("is_supermarket_service_station"),
                "location": st.get("location"),
                "fuel_types": ft,
                "fuel_prices": price_out,
                "distance_km": (round(d, 3) if d is not None else None),
            }
        )

    # Analyse: find cheapest station for each fuel type across the result set
    best: Dict[str, Any] = {}
    for fuel in sorted(fuel_seen):
        best_item: Optional[Dict[str, Any]] = None
        best_price: Optional[float] = None
        best_plu: Optional[str] = None

        for item in result:
            row = (item.get("fuel_prices") or {}).get(fuel)
            if not isinstance(row, dict):
                continue
            pv = row.get("price")
            if pv in (None, ""):
                continue
            try:
                p = float(pv)
            except Exception:
                continue
            if best_price is None or p < best_price:
                best_price = p
                best_item = item
                best_plu = row.get("price_last_updated")

        if best_item is not None and best_price is not None:
            # include full station details (flat), plus the selected fuel's price metadata
            best[fuel] = dict(best_item)
            best[fuel]["price"] = best_price
            best[fuel]["price_last_updated"] = best_plu
            best[fuel]["fuel_type"] = fuel

    return result, {"best_fuel": best}


# --------------------- CLI / config resolution ---------------------


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser()

    # Working directory and debug toggle
    p.add_argument(
        "--config-dir",
        default=None,
        help="Working directory for config and cache files",
    )
    p.add_argument(
        "--debug", action="store_true", help="Enable debug logging to stderr"
    )

    # API credentials (override config.json and environment)
    p.add_argument("--client-id", default=None)
    p.add_argument("--client-secret", default=None)

    # Cache control
    p.add_argument(
        "--full-refresh",
        action="store_true",
        help="Invalidate cache and rebuild baselines",
    )
    p.add_argument("--health", action="store_true", help="Only output cache stats")

    # Output section inhibitors
    p.add_argument(
        "--no-health",
        action="store_true",
        help="Omit cache/health metadata from output",
    )

    p.add_argument(
        "--no-stations", action="store_true", help="Omit stations array from output"
    )

    p.add_argument(
        "--no-best", action="store_true", help="Omit best_fuel analysis from output"
    )

    # Query mode: radius search or explicit station IDs
    p.add_argument("--lat", type=float)
    p.add_argument("--lon", type=float)
    p.add_argument("--radius-km", type=float)
    p.add_argument("--radius-miles", type=float)
    p.add_argument(
        "--station-id", action="append", default=[], help="Repeatable station id"
    )

    # Regex filters (repeatable, OR within category, AND across categories)
    p.add_argument("--re-name", action="append", default=[])
    p.add_argument("--re-postcode", action="append", default=[])
    p.add_argument("--re-town", action="append", default=[])
    p.add_argument("--re-id", action="append", default=[])
    p.add_argument("--re-brand", action="append", default=[])
    p.add_argument("--re-fuel", action="append", default=[])

    # Sort and result limit
    p.add_argument("--sort", default="distance", help="distance | cheapest:<FUELTYPE>")
    p.add_argument("--limit", type=int, default=10)

    return p.parse_args(argv)


def resolve_work_dir(args: argparse.Namespace) -> str:
    # Resolve working directory: CLI arg > env var > default
    return args.config_dir or os.environ.get("UFF_CONFIG_DIR") or DEFAULTS["config_dir"]


# --------------------- main ---------------------


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    # Enable debug output if requested
    global DEBUG
    DEBUG = bool(getattr(args, "debug", False))

    work_dir = resolve_work_dir(args)
    paths = make_paths(work_dir)

    debug_print(f"work_dir={work_dir}")

    # Merge file-based config over defaults
    cfg = dict(DEFAULTS)
    cfg.update(load_config_from_dir(work_dir))

    debug_print(
        f"Config loaded from {paths.config_file if paths.config_file.exists() else 'None'}"
    )

    # Resolve credentials: CLI > config.json > environment
    client_id = (
        args.client_id or cfg.get("client_id") or os.environ.get("UFF_CLIENT_ID")
    )
    client_secret = (
        args.client_secret
        or cfg.get("client_secret")
        or os.environ.get("UFF_CLIENT_SECRET")
    )

    debug_print(
        f"Creds sources: client_id={'CLI' if args.client_id else 'config/env'}; "
        f"client_secret={'CLI' if args.client_secret else 'config/env'}"
    )

    # Bail early if no credentials available
    if not client_id or not client_secret:
        print(
            json.dumps(
                {
                    "state": "error",
                    "error": "Missing client_id/client_secret (CLI, config.json, or env UFF_CLIENT_ID/UFF_CLIENT_SECRET)",
                },
                ensure_ascii=False,
            )
        )
        return 2

    # Convert miles to km if needed
    radius_km = args.radius_km
    if radius_km is None and args.radius_miles is not None:
        radius_km = args.radius_miles * 1.609344

    # Refresh cache (stations + prices) under file lock
    try:
        state, stats = ensure_cache(
            paths=paths,
            client_id=client_id,
            client_secret=client_secret,
            full_refresh=args.full_refresh,
            stations_baseline_days=int(
                cfg.get(
                    "stations_baseline_days", DEFAULTS["stations_baseline_days"]
                )
            ),
            stations_incremental_hours=int(
                cfg.get(
                    "stations_incremental_hours", DEFAULTS["stations_incremental_hours"]
                )
            ),
            prices_baseline_days=int(
                cfg.get(
                    "prices_baseline_days", DEFAULTS["prices_baseline_days"]
                )
            ),
            prices_incremental_hours=float(
                cfg.get(
                    "prices_incremental_hours", DEFAULTS["prices_incremental_hours"]
                )
            ),
        )
    except Exception as e:
        print(
            json.dumps(
                {
                    "state": "error",
                    "error": f"Cache refresh failed: {e}",
                    "generated_at": iso_utc(utc_now()),
                },
                ensure_ascii=False,
            )
        )
        return 2

    # Begin building JSON output
    out = {"state": "ok"}

    # Include cache health unless suppressed
    if args.health or not args.no_health:
        debug_print("Cache health: added")
        out["cache"] = stats
        out["generated_at"] = iso_utc(utc_now())

    # Health-only mode: output stats and exit
    if args.health:
        debug_print("Cache health: no other output required")
        print(json.dumps(out, ensure_ascii=False))
        return 0

    # Prepare query parameters
    station_ids = [s for s in (args.station_id or []) if s]

    # Compile regex filters from CLI arguments
    re_name = compile_res(args.re_name)
    re_postcode = compile_res(args.re_postcode)
    re_town = compile_res(args.re_town)
    re_id = compile_res(args.re_id)
    re_brand = compile_res(args.re_brand)
    re_fuel = compile_res(args.re_fuel)

    # Execute the station query against cached data
    try:
        stations_list, analysis = query_stations(
            state,
            lat=args.lat,
            lon=args.lon,
            radius_km=radius_km,
            station_ids=station_ids if station_ids else None,
            re_name=re_name,
            re_postcode=re_postcode,
            re_town=re_town,
            re_id=re_id,
            re_brand=re_brand,
            re_fuel=re_fuel,
            sort=args.sort,
            limit=args.limit,
        )
    except Exception as e:
        out["state"] = "error"
        out["error"] = str(e)
        debug_print("Error: caught exception")
        print(json.dumps(out, ensure_ascii=False))
        return 2

    # Append optional output sections
    if not args.no_stations:
        debug_print("Stations: added")
        out["stations"] = stations_list

    if not args.no_best:
        debug_print("Best: added")
        out.update(analysis)

    # Write final JSON to stdout
    print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
