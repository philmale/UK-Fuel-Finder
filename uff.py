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
    """Print a timestamped diagnostic message to stderr.

    Only emits output when the global DEBUG flag is True, so this can be called
    freely throughout the codebase without affecting normal JSON-only stdout output.

    Args:
        msg: The message to print.
    """
    if not DEBUG:
        return
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", file=sys.stderr)


# API base URL and HTTP status codes eligible for retry
BASE_URL = "https://www.fuel-finder.service.gov.uk"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

# Configurable defaults for cache timing, HTTP behaviour, and batch pacing.
# Note at peak times the API really needs to be treated with care to avoid
# hitting rate limits - experience shows that even 1 request per second can
# trigger 429s, so we use a conservative default with exponential backoff and jitter.
DEFAULTS = {
    "config_dir": "/config/.storage/uk_fuel_finder",
    "stations_baseline_days": 7,       # days between full stations pull
    "stations_incremental_hours": 12,  # hours between incremental stations updates
    "prices_baseline_days": 2,         # days between full prices pull
    "prices_incremental_hours": 1.0,   # hours between incremental price updates
    "http_timeout": 60,
    "http_retries": 6,
    "http_backoff_base": 1.8,
    "http_backoff_jitter": 0.7,
    "batch_sleep_seconds": 4.0,        # pause between API batch pages; be conservative
    "incremental_safety_minutes": 45,  # overlap window to catch late-arriving data
}


# --------------------- time helpers ---------------------


def utc_now() -> datetime:
    """Return the current UTC datetime as a timezone-aware object.

    Returns:
        Current UTC datetime with tzinfo set to timezone.utc.
    """
    return datetime.now(timezone.utc)


def iso_utc(dt: datetime) -> str:
    """Format a datetime as a UTC ISO 8601 string ending in 'Z'.

    Converts the supplied datetime to UTC, formats as ISO 8601, and replaces
    the '+00:00' suffix with 'Z' for compact, unambiguous representation.

    Args:
        dt: Any timezone-aware datetime.

    Returns:
        UTC ISO 8601 string, e.g. '2024-01-15T10:30:00Z'.
    """
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_dt_maybe(s: Optional[str]) -> Optional[datetime]:
    """Parse an ISO datetime string, returning None on any failure.

    Handles both 'Z'-terminated and '+00:00'-terminated strings.  Designed
    to be called on potentially missing or corrupt cache fields without raising.

    Args:
        s: ISO datetime string, or None.

    Returns:
        Timezone-aware datetime, or None if parsing fails.
    """
    if not s:
        return None
    
    # Replace trailing Z with +00:00 for fromisoformat compatibility (Python < 3.11)
    s2 = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s2)
    except Exception:
        return None


def parse_price_dt(s: Optional[str]) -> Optional[datetime]:
    """Parse an API price timestamp, treating naive datetimes as UTC.

    The Fuel Finder API returns price timestamps as 'YYYY-MM-DDTHH:MM:SS'
    without a timezone indicator.  We interpret these as UTC and normalise
    to a timezone-aware datetime for safe comparisons.

    Args:
        s: Price timestamp string from the API, or None.

    Returns:
        UTC timezone-aware datetime, or None if parsing fails.
    """
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        # If no timezone info present, assume UTC per spec
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


# --------------------- format helpers ---------------------


def format_address_line(location: Dict[str, Any], station_name: str = '') -> str:
    """Extract a concise, human-readable address line from raw API location data.

    The API address fields are inconsistently structured: sometimes a full
    address is crammed into address_line_1 as a comma-separated string; other
    times it is split across multiple fields.  This function applies heuristics
    to produce a useful 1–2 part location string (e.g. "High Street, York")
    by skipping noise words, postcodes, road numbers, and redundancy with the
    station name itself.

    Args:
        location: The station's 'location' dict from the API.
        station_name: The station's trading name, used to skip redundant address chunks.

    Returns:
        A comma-separated address string of up to two meaningful parts, or '' if
        nothing useful can be extracted.
    """
    # Generic noise words common in station names/addresses but not useful as location
    NOISE_WORDS = {'GARAGE', 'SERVICE', 'STATION', 'SERVICES', 'FILLING', 'PETROL', 'FORECOURT'}

    # Road descriptor words used both to identify useful address chunks and to
    # guard against skipping chunks that happen to share words with the station name
    ROAD_WORDS = {'ROAD', 'LANE', 'STREET', 'AVENUE', 'DRIVE', 'WAY', 'CLOSE',
                  'GROVE', 'PLACE', 'COURT', 'PARK', 'HILL', 'GREEN', 'BOULEVARD',
                  'SQUARE', 'TERRACE', 'CRESCENT', 'HIGHWAY', 'ROW', 'VIEW', 'WALK',
                  'CIRCLE', 'MOUNT', 'STRAND', 'WHARF', 'QUAY', 'PIKE', 'GATE', 'YARD',
                  'ESTATE', 'PARKWAY', 'MEWS', 'ALLEY', 'MALL', 'RING'}

    def is_road_number(s: str) -> bool:
        """Return True if s looks like a UK road identifier (e.g. A1, M6, B4420)."""
        s_clean = s.strip().upper()
        if len(s_clean) < 2 or len(s_clean) > 6:
            return False
        # UK road numbers start with A, B, or M followed by digits
        return s_clean[0] in 'ABM' and s_clean[1:].isdigit()

    def is_postcode(s: str) -> bool:
        """Return True if s resembles a UK postcode (with or without the space)."""
        s_clean = s.strip().replace(' ', '').upper()
        if not (5 <= len(s_clean) <= 8):
            return False
        # UK postcodes always end in digit-letter-letter (e.g. 7RP, 7DZ)
        return s_clean[-1].isalpha() and s_clean[-2].isalpha() and s_clean[-3].isdigit()

    def is_noise(s: str) -> bool:
        """Return True if s consists entirely of noise/filler words."""
        words = set(s.strip().upper().split())
        return bool(words) and words.issubset(NOISE_WORDS | {'&', 'AND', 'THE', 'LTD', 'LIMITED', 'PLC', 'LLP'})

    parts = []
    addr1 = (location.get('address_line_1') or '').strip()

    if addr1.count(',') >= 2:
        # Full address crammed into one field; split and process each chunk
        chunks = [c.strip() for c in addr1.split(',') if c.strip()]

        # Skip leading chunks that contain noise words (e.g. "TEXACO GARAGE",
        # "J R EWING & SON LTD") but stop as soon as we hit a road/location word.
        while chunks:
            chunk_words = set(chunks[0].upper().split())
            has_noise = bool(chunk_words & (NOISE_WORDS | {'LTD', 'LIMITED', 'PLC', 'LLP', 'CO', 'COMPANY'}))
            has_road = bool(chunk_words & ROAD_WORDS)
            if has_noise and not has_road:
                chunks = chunks[1:]
            else:
                break

        # If the station name appears in a chunk AND that chunk isn't a road,
        # skip everything up to and including that chunk to avoid repeating the
        # name in the address.  We don't skip road chunks even if they share
        # words with the name (e.g. "White Hart Lane Service Station").
        if station_name:
            name_upper = station_name.upper().strip()
            # Significant words: not noise, longer than 2 chars
            sig_words = [w for w in name_upper.split() if w not in NOISE_WORDS and len(w) > 2]
            for i, chunk in enumerate(chunks):
                chunk_upper = chunk.upper()
                chunk_words = set(chunk_upper.split())
                # Never skip a chunk that describes a road
                if chunk_words & ROAD_WORDS:
                    break
                full_match = name_upper in chunk_upper
                # Partial match: at least half of the significant words appear
                word_match = sig_words and sum(
                    1 for w in sig_words if w in chunk_upper
                ) >= max(1, len(sig_words) // 2)
                if full_match or word_match:
                    # Skip up to and including this name-containing chunk
                    chunks = chunks[i + 1:]
                    break

        # Pick up to two meaningful location chunks, skipping postcodes, road
        # numbers, and pure noise strings
        for chunk in chunks:
            if is_postcode(chunk) or is_road_number(chunk) or is_noise(chunk):
                continue
            parts.append(chunk)
            if len(parts) >= 2:
                break
    else:
        # Normal multi-field address; iterate in preference order
        for field in ['address_line_1', 'address_line_2', 'city']:
            val = (location.get(field) or '').strip()
            if val and val.lower() != 'null':
                if not is_road_number(val) and not is_noise(val):
                    parts.append(val)
                    if len(parts) >= 2:
                        break

    return ', '.join(parts[:2]) if parts else ''


# --------------------- geo helpers ---------------------


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great-circle distance between two points on Earth.

    Uses the Haversine formula, which is accurate to within ~0.3% for the
    distances involved in fuel station proximity searches.

    Args:
        lat1: Latitude of point 1 in decimal degrees.
        lon1: Longitude of point 1 in decimal degrees.
        lat2: Latitude of point 2 in decimal degrees.
        lon2: Longitude of point 2 in decimal degrees.

    Returns:
        Distance in kilometres.
    """
    # Mean Earth radius in km (WGS-84 mean radius)
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


# --------------------- filesystem / locking ---------------------


@dataclass
class Paths:
    """Resolved filesystem paths for all files in the working directory.

    Attributes:
        work_dir:    Root working directory.
        state_file:  Cached station and price data (state.json).
        token_file:  OAuth token storage (token.json).
        lock_file:   Inter-process lock file (state.lock).
        config_file: Optional user configuration (config.json).
    """
    work_dir: Path
    state_file: Path
    token_file: Path
    lock_file: Path
    config_file: Path


def make_paths(work_dir: str) -> Paths:
    """Derive all working file paths from a single directory string.

    Args:
        work_dir: Absolute or relative path to the working directory.

    Returns:
        A Paths dataclass with all derived file paths populated.
    """
    d = Path(work_dir)
    return Paths(
        work_dir=d,
        state_file=d / "state.json",
        token_file=d / "token.json",
        lock_file=d / "state.lock",
        config_file=d / "config.json",
    )


class FileLock:
    """Exclusive advisory file lock using fcntl, for multi-process safety.

    Multiple Home Assistant command_line sensors may invoke this script
    concurrently.  The lock prevents two processes from writing to state.json
    simultaneously, which would corrupt the cache.

    Usage::

        with FileLock(paths.lock_file):
            # safe to read and write state here
    """

    def __init__(self, lock_path: Path) -> None:
        """Initialise with the path to use as the lock file.

        Args:
            lock_path: Path to create and lock (does not need to exist).
        """
        self.lock_path = lock_path
        self.fd = None

    def __enter__(self) -> FileLock:
        """Acquire an exclusive lock, blocking until it is available.

        Creates the parent directory if necessary, opens the lock file,
        and blocks via LOCK_EX until no other process holds the lock.

        Returns:
            self, for use in 'with' statements.
        """
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self.fd = open(self.lock_path, "w")
        fcntl.flock(self.fd, fcntl.LOCK_EX)
        return self

    def __exit__(
        self, exc_type: Optional[type], exc: Optional[BaseException], tb: Optional[Any]
    ) -> None:
        """Release the lock and close the file descriptor.

        Always releases the lock even if an exception occurred inside the
        'with' block, to prevent deadlocks between concurrent sensor processes.

        Args:
            exc_type: Exception class, or None.
            exc:      Exception instance, or None.
            tb:       Traceback, or None.
        """
        try:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
        finally:
            try:
                self.fd.close()
            except Exception:
                pass


# --------------------- http / retries ---------------------


class AuthError(Exception):
    """Raised when the API returns a 401 or 403 response.

    Separates authentication failures from transient server errors so that
    the retry logic can handle them differently: auth errors trigger a token
    refresh rather than a simple backoff-and-retry.

    Attributes:
        response: The requests.Response that triggered the error, if available.
    """

    def __init__(
        self, message: str, response: Optional[requests.Response] = None
    ) -> None:
        """Initialise with an error message and optional response object.

        Args:
            message:  Human-readable description of the auth failure.
            response: The HTTP response that caused the error.
        """
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
    """Execute an HTTP request with exponential backoff retry for transient failures.

    Retries on network errors and on HTTP status codes in RETRYABLE_STATUS
    (429, 5xx).  Auth errors (401, 403) are raised immediately without retry
    so that the caller can obtain a new token before re-attempting.

    Sleep duration is calculated as::

        min(30, backoff_base ** attempt + backoff_jitter * (0.5 + attempt % 3 / 3))

    This produces a gently increasing delay with a small jitter to spread
    load from concurrent sensor processes hitting the API simultaneously.

    Args:
        method:         HTTP method string (e.g. 'GET', 'POST').
        url:            Full URL to request.
        headers:        Request headers dict.
        params:         Optional query string parameters.
        json_body:      Optional JSON request body (sent as application/json).
        timeout:        Request timeout in seconds.
        retries:        Maximum number of attempts (including the first).
        backoff_base:   Base for exponential backoff calculation.
        backoff_jitter: Multiplier for the jitter term.

    Returns:
        A successful requests.Response object.

    Raises:
        AuthError:               On HTTP 401 or 403 (not retried).
        requests.HTTPError:      On non-retryable HTTP errors after all attempts.
        requests.RequestException: On network-level failures after all attempts.
    """
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

            # Auth failures are not retryable; raise immediately for token refresh
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
            # Auth errors propagate immediately without retry
            if isinstance(e, AuthError):
                raise

            last_exc = e

            # Final attempt exhausted; re-raise so caller can handle
            if attempt == retries - 1:
                raise
            try:
                status = getattr(getattr(e, "response", None), "status_code", None)
            except Exception:
                status = None
            debug_print(
                f"HTTP retry {attempt + 1}/{retries - 1} for {method} {url} (status={status}): {e}"
            )
            # Exponential backoff with per-attempt jitter, capped at 30s
            sleep_s = (backoff_base**attempt) + (
                backoff_jitter * (0.5 + (attempt % 3) / 3)
            )
            time.sleep(min(30.0, sleep_s))
    if last_exc:
        raise last_exc
    raise RuntimeError("request_with_retry: unreachable")


# --------------------- json helpers ---------------------


def load_json(path: Path) -> Optional[Dict[str, Any]]:
    """Load and parse a JSON file, returning None if the file is missing or corrupt.

    Designed to be safe for reading potentially absent or partially-written
    cache files without raising exceptions.

    Args:
        path: Path to the JSON file.

    Returns:
        Parsed dict, or None on any error.
    """
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_json(path: Path, data: Dict[str, Any]) -> None:
    """Write a dict to a JSON file, creating parent directories as needed.

    Writes the full file in one call.  For truly atomic writes a temporary
    file + rename would be needed, but for our use case the file lock provides
    sufficient protection against concurrent corruption.

    Args:
        path: Destination file path.
        data: Dict to serialise.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def load_config(path: str) -> Dict[str, Any]:
    """Load a JSON configuration file, returning an empty dict on any failure.

    Args:
        path: Path to the config file as a string.

    Returns:
        Configuration dict, or {} if the file is absent, unreadable, or invalid.
    """
    p = Path(path)
    if not p.exists():
        return {}
    try:
        v = json.loads(p.read_text(encoding="utf-8"))
        # Only accept a top-level dict; reject arrays or scalars
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


def load_config_from_dir(work_dir: str) -> Dict[str, Any]:
    """Convenience wrapper: load config.json from the given working directory.

    Args:
        work_dir: Path to the working directory.

    Returns:
        Configuration dict, or {} if config.json is absent or invalid.
    """
    cfg_path = Path(work_dir) / "config.json"
    if not cfg_path.exists():
        return {}
    return load_config(str(cfg_path))


# --------------------- OAuth token manager ---------------------


def get_access_token(
    paths: Paths, client_id: str, client_secret: str, *, force_refresh: bool = False
) -> str:
    """Obtain a valid OAuth access token, using the cache where possible.

    Token acquisition strategy (in order):
    1. Return the cached access token if it is still valid (with a 30s margin).
    2. Use the refresh token to obtain a new access token without client_secret.
    3. Fall back to a full credential exchange if refresh fails or is absent.

    Persists token data to token.json after each successful acquisition so
    that subsequent invocations (including from concurrent HA sensors) can
    reuse the same token.

    Args:
        paths:         Resolved file paths for the working directory.
        client_id:     API client identifier.
        client_secret: API client secret.
        force_refresh: If True, skip the cached token check and re-acquire.

    Returns:
        A valid access token string.

    Raises:
        Various requests exceptions if all token acquisition attempts fail.
    """
    token = load_json(paths.token_file) or {}
    access = token.get("access_token")
    expires_at = parse_dt_maybe(token.get("expires_at"))
    refresh = token.get("refresh_token")

    # Use cached token if still valid (with 30s safety margin to avoid using
    # a token that expires mid-request)
    if (
        (not force_refresh)
        and access
        and expires_at
        and utc_now() < (expires_at - timedelta(seconds=30))
    ):
        debug_print("OAuth: using cached access_token")
        return access

    # Try refresh token flow first — avoids sending the client_secret on the wire
    if refresh:
        try:
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
            # API may wrap the token in a 'data' envelope or return it directly
            token_data = data.get("data", data)

            access_token = token_data["access_token"]
            expires_in = int(token_data.get("expires_in", 3600))

            # Persist new access token, keeping the existing refresh token
            new_token = {
                "access_token": access_token,
                "refresh_token": refresh,
                "expires_at": iso_utc(utc_now() + timedelta(seconds=expires_in)),
                "token_type": token_data.get("token_type", "Bearer"),
                "updated_at": iso_utc(utc_now()),
            }
            save_json(paths.token_file, new_token)
            return access_token
        except Exception as e:
            debug_print(f"OAuth: refresh failed ({e}), falling back to full token generation")
            # Fall through to full credential exchange below

    # No valid refresh token available; perform full client credentials exchange
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

    # Persist both access and refresh tokens for future use
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
    """Fetch all pages from a paginated API endpoint, returning the combined rows.

    The Fuel Finder API returns results in batches of up to 500 rows each.
    This function iterates through batch numbers (starting at 1) until a batch
    returns fewer than 500 rows, which indicates the final page.

    If the token expires mid-pagination, ``refresh_token_fn`` is called to
    obtain a new token and the failed batch is retried once.

    Args:
        token:             Bearer access token for authorisation.
        path:              API path, e.g. '/api/v1/pfs/fuel-prices'.
        params:            Additional query parameters (e.g. timestamp filters).
        batch_sleep:       Seconds to sleep between batch requests to avoid
                           rate limiting.
        refresh_token_fn:  Callable that returns a fresh access token; used to
                           recover from mid-pagination auth failures.

    Returns:
        A flat list of all row dicts across all pages.
    """
    t0 = time.time()
    debug_print(f"API fetch start: {path} params={params}")
    headers = {"accept": "application/json", "authorization": f"Bearer {token}"}
    out: List[Dict[str, Any]] = []
    batch = 1
    while True:
        # Build query params for this page; batch-number is 1-indexed
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
            token = refresh_token_fn()  # obtain new access token
            headers = {"accept": "application/json", "authorization": f"Bearer {token}"}
            resp = request_with_retry("GET", url, headers=headers, params=qp)

        response_body = resp.json()
        # Handle both wrapped ({'success': True, 'data': [...]}) and unwrapped ([...]) responses
        if isinstance(response_body, dict) and 'data' in response_body:
            data = response_body.get('data', [])
        elif isinstance(response_body, list):
            data = response_body
        else:
            data = []

        if not isinstance(data, list):
            data = []
        out.extend(data)
        debug_print(f"  batch {batch}: {len(data)} rows (total {len(out)})")
        # A batch of fewer than 500 rows means this was the last page
        if len(data) < 500:
            break
        batch += 1
        # Pause between pages to stay well within the API's rate limits
        time.sleep(batch_sleep)
    debug_print(f"API fetch done: {path} rows={len(out)} in {time.time() - t0:.2f}s")
    return out


# --------------------- cache model ---------------------


def empty_state() -> Dict[str, Any]:
    """Return an initialised blank state structure with all required keys.

    This defines the canonical shape of state.json.  All keys must be present
    to avoid KeyError or AttributeError when the cache is freshly created or
    has been invalidated.

    Returns:
        A dict with empty stations/prices dicts and default meta fields.
    """
    return {
        "stations": {},  # node_id -> station object
        "prices": {},    # node_id -> {fuel_type -> {price, price_last_updated}}
        "meta": {
            "stations_baseline_at": None,
            "stations_last_incremental_at": None,
            "prices_baseline_at": None,
            "prices_last_incremental_at": None,
            "prices_max_price_last_updated": None,
            "price_fix_count_total": 0,
            "price_fix_count_last_run": 0,
            "price_fix_last_run_at": None,
            "created_at": iso_utc(utc_now()),
            "updated_at": iso_utc(utc_now()),
        },
    }


def load_state(paths: Paths) -> Dict[str, Any]:
    """Load the cached state from disk, filling in any missing meta keys.

    Adds any new meta keys with safe defaults so that the schema can be
    extended without invalidating existing cache files (forward-compatible).

    Args:
        paths: Resolved file paths for the working directory.

    Returns:
        The loaded state dict, or a fresh empty_state() if the file is
        missing, corrupt, or has an unexpected structure.
    """
    data = load_json(paths.state_file)
    if (
        isinstance(data, dict)
        and "stations" in data
        and "prices" in data
        and "meta" in data
    ):
        # Back-fill any meta keys added in later versions of the script
        meta = data.get("meta", {}) or {}
        meta.setdefault("price_fix_count_total", 0)
        meta.setdefault("price_fix_count_last_run", 0)
        meta.setdefault("price_fix_last_run_at", None)
        data["meta"] = meta
        return data
    # File missing, corrupt, or wrong shape — start fresh
    return empty_state()


def save_state(paths: Paths, state: Dict[str, Any]) -> None:
    """Persist the current state to disk, updating the modified timestamp.

    Args:
        paths: Resolved file paths for the working directory.
        state: State dict to serialise.
    """
    state["meta"]["updated_at"] = iso_utc(utc_now())
    save_json(paths.state_file, state)


def invalidate_cache(paths: Paths) -> None:
    """Delete the existing state file and replace it with an empty state.

    Used by --full-refresh to force re-download of all baseline data.

    Args:
        paths: Resolved file paths for the working directory.
    """
    paths.work_dir.mkdir(parents=True, exist_ok=True)
    if paths.state_file.exists():
        paths.state_file.unlink()
    save_state(paths, empty_state())


def cache_stats(state: Dict[str, Any]) -> Dict[str, Any]:
    """Build a summary dict of cache health metrics for inclusion in JSON output.

    Collects counts, timestamps, and price-fix statistics from the state meta
    block.  File sizes are added by the caller after this function returns.

    Args:
        state: The current state dict.

    Returns:
        A flat dict of cache health fields.
    """
    stations = state.get("stations", {}) or {}
    prices = state.get("prices", {}) or {}
    # Enumerate all fuel types seen across all priced stations
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
        "price_fix_count_total": int(meta.get("price_fix_count_total") or 0),
        "price_fix_count_last_run": int(meta.get("price_fix_count_last_run") or 0),
        "price_fix_last_run_at": meta.get("price_fix_last_run_at"),
        "state_file_bytes": None,
        "token_file_bytes": None,
    }


# --------------------- transform / merge ---------------------


def stations_to_dict(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Convert a flat list of station objects to a dict keyed by node_id.

    Stations without a node_id are silently skipped.

    Args:
        items: Raw list of station dicts from the API.

    Returns:
        Dict mapping str(node_id) -> station dict.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for it in items:
        sid = it.get("node_id")
        if not sid:
            continue
        out[str(sid)] = it
    return out


def merge_station_dict(base: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    """Upsert incremental station updates into the existing cache dict.

    New stations are inserted; existing stations are replaced with the
    updated record.  No stations are deleted during an incremental merge
    (deletions are only handled during a baseline rebuild).

    Args:
        base:    Existing stations dict (node_id -> station).
        updates: New/updated stations from the incremental fetch.

    Returns:
        The merged stations dict.
    """
    base = dict(base or {})
    for sid, obj in (updates or {}).items():
        base[str(sid)] = obj
    return base


def _price_fix_to_pence(price_raw: Any) -> Tuple[Any, int]:
    """Detect and correct fuel prices incorrectly entered in pounds rather than pence.

    The API specification requires prices in pence per litre (e.g. 145.9 for
    £1.459/litre).  Some stations occasionally submit prices in pounds (e.g. 1.459),
    which would appear as sub-2p fuel.  Any price below 2 is assumed to be in
    pounds and is multiplied by 100, rounded to one decimal place.

    Args:
        price_raw: Raw price value from the API (may be int, float, str, or None).

    Returns:
        A tuple of (corrected_price, fix_count) where fix_count is 1 if a
        correction was applied, 0 otherwise.  Returns (price_raw, 0) unchanged
        if the value is None, empty, or cannot be parsed.
    """
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

    # Normal pence value: normalise to float for JSON serialisation consistency
    try:
        return float(d), 0
    except Exception:
        return price_raw, 0


def prices_to_dict(
    items: List[Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], Optional[str], int]:
    """Convert a flat list of API price records into a nested station->fuel->price dict.

    Also tracks the most recent price timestamp across all records (used as the
    incremental cursor for subsequent fetches) and counts any price-entry
    corrections applied.

    Args:
        items: Raw list of price records from the API, each containing a
               node_id and a fuel_prices list.

    Returns:
        A three-tuple of:
        - prices dict: str(node_id) -> {fuel_type -> {price, price_last_updated}}
        - max_price_last_updated: ISO UTC string of the newest price timestamp,
          or None if no valid timestamps were found.
        - fix_count: number of pound-to-pence corrections applied.
    """
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
        # Retrieve any existing prices for this station to merge into
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

            # Advance the cursor to the newest price timestamp seen so far
            dt = parse_price_dt(plu)
            if dt and (max_dt is None or dt > max_dt):
                max_dt = dt
        out[sid] = per_station

    return out, (iso_utc(max_dt) if max_dt else None), fix_count


def merge_price_dict(base: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    """Upsert incremental price updates into the existing prices cache.

    For each station in the updates, individual fuel type entries are replaced.
    Fuel types not present in the update are left unchanged.

    Args:
        base:    Existing prices dict (node_id -> fuel_type -> price_row).
        updates: New/updated prices from the incremental fetch.

    Returns:
        The merged prices dict.
    """
    base = dict(base or {})
    for sid, fuels in (updates or {}).items():
        sid = str(sid)
        # Initialise station entry if not already present
        if sid not in base or not isinstance(base.get(sid), dict):
            base[sid] = {}
        for ft, row in (fuels or {}).items():
            base[sid][str(ft)] = row
    return base


# --------------------- refresh policy ---------------------


def needs_stations_baseline(state: Dict[str, Any], days: int) -> bool:
    """Return True if the stations baseline has never run or has expired.

    Args:
        state: Current state dict.
        days:  Maximum age of the baseline in days before re-pulling.

    Returns:
        True if a full stations pull is required.
    """
    dt = parse_dt_maybe(state.get("meta", {}).get("stations_baseline_at"))
    if dt is None:
        return True
    return utc_now() >= (dt + timedelta(days=days))


def needs_prices_baseline(state: Dict[str, Any], days: int) -> bool:
    """Return True if the prices baseline has never run or has expired.

    Args:
        state: Current state dict.
        days:  Maximum age of the baseline in days before re-pulling.

    Returns:
        True if a full prices pull is required.
    """
    dt = parse_dt_maybe(state.get("meta", {}).get("prices_baseline_at"))
    if dt is None:
        return True
    return utc_now() >= (dt + timedelta(days=days))


def needs_stations_incremental(state: Dict[str, Any], hours: int) -> bool:
    """Return True if the stations incremental update is due.

    Args:
        state: Current state dict.
        hours: Maximum age of the last incremental fetch in hours.

    Returns:
        True if an incremental stations fetch is required.
    """
    dt = parse_dt_maybe(state.get("meta", {}).get("stations_last_incremental_at"))
    if dt is None:
        return True
    return utc_now() >= (dt + timedelta(hours=hours))


def needs_prices_incremental(state: Dict[str, Any], hours: float) -> bool:
    """Return True if the prices incremental update is due.

    Args:
        state: Current state dict.
        hours: Maximum age of the last incremental fetch in hours (fractional allowed).

    Returns:
        True if an incremental prices fetch is required.
    """
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
    prices_refresh: bool = False,
    stations_baseline_days: int,
    stations_incremental_hours: int,
    prices_baseline_days: int,
    prices_incremental_hours: float,
    batch_sleep_seconds: float = DEFAULTS["batch_sleep_seconds"],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Ensure the local cache is up to date, fetching from the API as needed.

    Implements a two-tier refresh strategy for both stations and prices:

    **Baseline** (periodic full pull):
      Downloads all records from the API and replaces the cache wholesale.
      Expensive (many API batches) but necessary to catch deletions and to
      establish a clean baseline after the cache has aged out.

    **Incremental** (frequent delta pull):
      Fetches only records changed since the last pull, using a timestamp
      cursor with a configurable safety overlap to catch late-arriving data.
      Much cheaper and runs on every invocation when the baseline is current.

    A new stations baseline always triggers a prices baseline too, to keep
    the two datasets in sync and prune orphaned price entries.

    The entire update is wrapped in an exclusive file lock to prevent two
    concurrent HA sensor processes from corrupting the cache.

    Args:
        paths:                    Resolved file paths.
        client_id:                API OAuth client ID.
        client_secret:            API OAuth client secret.
        full_refresh:             If True, wipe cache and rebuild from scratch.
        prices_refresh:           If True, force a prices baseline only.
        stations_baseline_days:   Days between stations full pulls.
        stations_incremental_hours: Hours between stations incremental pulls.
        prices_baseline_days:     Days between prices full pulls.
        prices_incremental_hours: Hours between prices incremental pulls.
        batch_sleep_seconds:      Pause between API batch pages.

    Returns:
        A tuple of (state dict, cache_stats dict).  The stats dict includes
        file sizes for the state and token files.
    """
    # Acquire exclusive lock to prevent concurrent cache writes from HA sensors
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
            """Re-acquire the access token unconditionally; used after mid-run auth errors."""
            return get_access_token(paths, client_id, client_secret, force_refresh=True)

        # Reset the per-run fix counter; will be updated if prices are fetched
        state["meta"]["price_fix_count_last_run"] = 0

        # Track whether stations baseline ran so we can force a prices baseline
        did_stations_baseline = False

        # --- Stations baseline (full pull, periodic) ---
        if needs_stations_baseline(state, stations_baseline_days):
            baseline_at = state.get("meta", {}).get("stations_baseline_at")
            dt = parse_dt_maybe(baseline_at)
            age_days = (utc_now() - dt).days if dt else None
            debug_print(
                f"Stations: baseline refresh required "
                f"(last_baseline={baseline_at or 'never'}, "
                f"age={age_days}d if age_days else 'never', "
                f"threshold={stations_baseline_days}d)"
            )
            items = fetch_all_batches(
                token,
                "/api/v1/pfs",
                refresh_token_fn=force_refresh_access_token,
                batch_sleep=batch_sleep_seconds,
            )
            state["stations"] = stations_to_dict(items)
            debug_print(
                f"Stations: baseline loaded {len(items)} rows; "
                f"stations now {len(state['stations'])}"
            )
            now = iso_utc(utc_now())
            state["meta"]["stations_baseline_at"] = now
            # Advance incremental cursor to now so the next incremental pull
            # only fetches changes since this baseline
            state["meta"]["stations_last_incremental_at"] = now
            debug_print(
                f"Stations: cursor advanced to {state['meta']['stations_last_incremental_at']}"
            )
            # Flag so the prices section knows to baseline too
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

            # Wind back the cursor by the safety margin to catch late-arriving records
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
                batch_sleep=batch_sleep_seconds,
            )

            # Merge any new or updated stations into the existing cache
            if items:
                upd = stations_to_dict(items)
                before = len(state.get("stations", {}) or {})
                state["stations"] = merge_station_dict(state["stations"], upd)
                after = len(state.get("stations", {}) or {})
                debug_print(
                    f"Stations: merged {len(upd)} updates (stations {before}->{after})"
                )

            # Advance cursor to now regardless of whether any records came back
            state["meta"]["stations_last_incremental_at"] = iso_utc(utc_now())
            debug_print(
                f"Stations: cursor advanced to {state['meta']['stations_last_incremental_at']}"
            )
        else:
            debug_print("Stations: no refresh needed")

        # --- Prices baseline (full pull, also forced after stations baseline) ---
        if did_stations_baseline or prices_refresh or needs_prices_baseline(state, prices_baseline_days):
            baseline_at = state.get("meta", {}).get("prices_baseline_at")
            debug_print(
                f"Prices: baseline refresh required "
                f"(did_stations_baseline={did_stations_baseline}, "
                f"last_baseline={baseline_at or 'never'}, "
                f"threshold={prices_baseline_days}d)"
            )
            items = fetch_all_batches(
                token,
                "/api/v1/pfs/fuel-prices",
                refresh_token_fn=force_refresh_access_token,
                batch_sleep=batch_sleep_seconds,
            )
            prices, max_plu, fix_count = prices_to_dict(items)
            state["prices"] = prices
            debug_print(
                f"Prices: baseline loaded {len(items)} rows; "
                f"prices now {len(state['prices'])}; max_plu={max_plu}; fixes={fix_count}"
            )
            now = iso_utc(utc_now())
            state["meta"]["prices_baseline_at"] = now
            # Use the newest price timestamp as the incremental cursor so the
            # next incremental pull only fetches prices changed after baseline
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

            # After a stations baseline, prune price entries for stations that
            # no longer exist in the API (avoids stale data in query results)
            if did_stations_baseline:
                station_ids = set((state.get("stations") or {}).keys())
                price_ids = set((state.get("prices") or {}).keys())

                orph_prices = sorted(price_ids - station_ids)
                orph_stns = sorted(station_ids - price_ids)

                debug_print(
                    f"Cache: orphan prices (no station): {len(orph_prices)}/{len(price_ids)}; "
                    f"orphan stations (no price): {len(orph_stns)}/{len(station_ids)}"
                )

                # Remove price entries with no matching station record
                if orph_prices:
                    for sid in orph_prices:
                        del state["prices"][sid]
                    debug_print(
                        f"Cache: pruned {len(orph_prices)} orphan price entries"
                    )
                # Leave orphan stations intact; their prices may arrive in the
                # next incremental pull if the API has a processing delay.
                # (Commented-out block retained for reference only)
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
            # Wind back the cursor by the safety margin to catch late-arriving updates
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
                batch_sleep=batch_sleep_seconds,
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
                # If the API returned price timestamps, advance cursor to the newest;
                # otherwise fall back to wall-clock time to avoid re-fetching the same window
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

                # Update price-fix counters for this run
                state["meta"]["price_fix_count_last_run"] = int(fix_count)
                state["meta"]["price_fix_count_total"] = int(
                    state["meta"].get("price_fix_count_total") or 0
                ) + int(fix_count)
                state["meta"]["price_fix_last_run_at"] = iso_utc(utc_now())
            else:
                # No incremental data returned; still advance cursor to avoid re-querying
                state["meta"]["prices_last_incremental_at"] = iso_utc(utc_now())
                debug_print(
                    f"Prices: cursor advanced to {state['meta']['prices_last_incremental_at']}"
                )
                state["meta"]["price_fix_count_last_run"] = 0

        else:
            debug_print("Prices: no refresh needed")
            state["meta"]["price_fix_count_last_run"] = 0

        save_state(paths, state)

    # Build cache health stats outside the lock (no writes needed)
    stats = cache_stats(state)
    try:
        # Append file sizes for diagnostic purposes
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
    """Compile a list of regex strings into case-insensitive Pattern objects.

    Args:
        res: List of regex strings, or None/empty list.

    Returns:
        List of compiled patterns, or None if the input was empty/None.
        None is used as a sentinel meaning "no filter applied" (pass-through).
    """
    if not res:
        return None
    out: List[re.Pattern] = []
    for s in res:
        out.append(re.compile(s, re.IGNORECASE))
    return out


def any_match(patterns: Optional[List[re.Pattern]], value: Optional[str]) -> bool:
    """Test whether a value matches any of the supplied patterns.

    When patterns is None (no filter configured), returns True unconditionally
    so that the calling code can use this as a simple pass-through gate.

    Args:
        patterns: Compiled regex patterns, or None to skip filtering.
        value:    String to test, or None (treated as empty string).

    Returns:
        True if patterns is None or any pattern matches the value.
    """
    if patterns is None:
        return True
    v = value or ""
    return any(p.search(v) for p in patterns)


def fuels_match(
    patterns: Optional[List[re.Pattern]], fuel_types: Iterable[str]
) -> bool:
    """Test whether any of a station's fuel types matches any of the supplied patterns.

    Used to implement the --re-fuel filter, which selects stations that offer
    at least one matching fuel type.

    Args:
        patterns:   Compiled regex patterns, or None to skip filtering.
        fuel_types: Iterable of fuel type strings from a station record.

    Returns:
        True if patterns is None, or any pattern matches any fuel type.
    """
    if patterns is None:
        return True
    # Guard against None values in the fuel_types iterable
    fts = [str(x) for x in fuel_types if x is not None]
    return any(p.search(ft) for p in patterns for ft in fts)


def station_field(st: Dict[str, Any], which: str) -> str:
    """Extract a named text field from a station dict for regex matching.

    Centralises field access so that filtering code does not need to know
    the exact API key paths for each filterable attribute.

    Args:
        st:    Station dict from the cache.
        which: Logical field name: 'station_id', 'name', 'brand', 'town', or 'postcode'.

    Returns:
        The field value as a string, or '' if missing or unrecognised.
    """
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
    """Extract the (latitude, longitude) pair from a station record.

    Args:
        st: Station dict from the cache.

    Returns:
        (lat, lon) float tuple, or None if the coordinates are absent or invalid.
    """
    loc = st.get("location") or {}
    try:
        lat = float(loc.get("latitude"))
        lon = float(loc.get("longitude"))
        return lat, lon
    except Exception:
        return None


def station_fuel_types(st: Dict[str, Any]) -> List[str]:
    """Return the list of fuel type codes offered by a station.

    Args:
        st: Station dict from the cache.

    Returns:
        List of fuel type strings (e.g. ['E10', 'E5', 'B7']), or [] if absent.
    """
    f = st.get("fuel_types", [])
    if isinstance(f, list):
        return [str(x) for x in f if x is not None]
    return []


def station_price_for(
    state: Dict[str, Any], station_id: str, fuel_type: str
) -> Optional[float]:
    """Look up the cached price for a specific fuel at a specific station.

    Args:
        state:      Current state dict.
        station_id: Station node_id string.
        fuel_type:  Fuel type code (e.g. 'E10').

    Returns:
        Price in pence per litre as a float, or None if not available.
    """
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
    """Query the cached station data with filtering, sorting, and result limiting.

    Supports two mutually exclusive selection modes:
    - **Radius search**: finds all stations within radius_km of (lat, lon).
    - **ID list**: returns only the explicitly specified station IDs.

    Regex filters are applied as AND across categories (name AND postcode AND
    brand etc.) with OR semantics within each category (any pattern in a
    category can match).

    The best (cheapest) station for each fuel type across the result set is
    included in the returned analysis dict.

    Args:
        state:        Current state dict.
        lat:          Centre latitude for radius search.
        lon:          Centre longitude for radius search.
        radius_km:    Search radius in kilometres.
        station_ids:  Explicit station IDs to retrieve (skips radius logic).
        re_name:      Compiled name filter patterns, or None.
        re_postcode:  Compiled postcode filter patterns, or None.
        re_town:      Compiled town/city filter patterns, or None.
        re_id:        Compiled station_id filter patterns, or None.
        re_brand:     Compiled brand filter patterns, or None.
        re_fuel:      Compiled fuel type filter patterns, or None.
        sort:         Sort mode: 'distance' or 'cheapest:<FUEL_TYPE>'.
        limit:        Maximum number of results to return.

    Returns:
        A tuple of:
        - stations_list: List of station dicts enriched with distance,
          current prices, and a formatted address line.
        - analysis: Dict containing 'best_fuel': a mapping of fuel_type ->
          cheapest station dict with price metadata attached.

    Raises:
        ValueError: If radius search parameters are incomplete.
    """
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

    # Apply regex filters: AND logic across filter types, OR within each type
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

    # Sort results: by distance (default), cheapest price, or fallback to distance
    sort_key = (sort or "").strip()

    def dist_val(d: Optional[float]) -> float:
        """Return the distance, or infinity for stations without a known distance."""
        return d if d is not None else float("inf")

    if sort_key == "distance" or sort_key == "":
        filtered.sort(key=lambda x: dist_val(x[1]))
    elif sort_key.startswith("cheapest:"):
        # Sort by price ascending for the specified fuel; distance as tiebreaker
        fuel = sort_key.split(":", 1)[1].strip()

        def key(item: Tuple[Dict[str, Any], Optional[float]]) -> Tuple[float, float]:
            st, d = item
            sid = str(st.get("node_id"))
            pv = station_price_for(state, sid, fuel)
            # Stations without a price for this fuel sort last
            return (pv if pv is not None else float("inf"), dist_val(d))

        filtered.sort(key=key)
    else:
        # Unrecognised sort key; fall back to distance to avoid silent incorrect ordering
        filtered.sort(key=lambda x: dist_val(x[1]))

    # Apply result count limit
    filtered = filtered[: max(0, limit)]
    debug_print(
        f"Query: sort='{sort_key or 'distance'}' limit={limit} returning={len(filtered)}"
    )

    # Build enriched output list and collect all fuel types present in results
    result: List[Dict[str, Any]] = []
    fuel_seen: set[str] = set()

    for st, d in filtered:
        sid = str(st.get("node_id"))
        ft = station_fuel_types(st)
        fuel_seen |= set(ft)

        # Attach current prices for each fuel type offered at this station
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
                "address_display": format_address_line(
                    st.get("location") or {},
                    st.get("trading_name") or ''
                ),
                "fuel_types": ft,
                "fuel_prices": price_out,
                # Round to 3dp to keep JSON tidy while preserving meaningful precision
                "distance_km": (round(d, 3) if d is not None else None),
            }
        )

    # Find the cheapest station for each fuel type across the result set
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
            # Include the full station record plus the winning fuel's price metadata
            best[fuel] = dict(best_item)
            best[fuel]["price"] = best_price
            best[fuel]["price_last_updated"] = best_plu
            best[fuel]["fuel_type"] = fuel

    return result, {"best_fuel": best}


# --------------------- CLI / config resolution ---------------------


def parse_args(argv: List[str]) -> argparse.Namespace:
    """Define and parse all CLI arguments.

    Args:
        argv: Argument list (typically sys.argv[1:]).

    Returns:
        Parsed argparse.Namespace.
    """
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

    # Cache control flags
    p.add_argument(
        "--full-refresh",
        action="store_true",
        help="Invalidate cache and rebuild baselines",
    )
    p.add_argument(
        "--prices-refresh",
        action="store_true",
        help="Force prices baseline refresh only (keep stations cache)",
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

    # Regex filters (repeatable; OR within each category, AND across categories)
    p.add_argument("--re-name", action="append", default=[])
    p.add_argument("--re-postcode", action="append", default=[])
    p.add_argument("--re-town", action="append", default=[])
    p.add_argument("--re-id", action="append", default=[])
    p.add_argument("--re-brand", action="append", default=[])
    p.add_argument("--re-fuel", action="append", default=[])

    # Sort mode and result count
    p.add_argument("--sort", default="distance", help="distance | cheapest:<FUELTYPE>")
    p.add_argument("--limit", type=int, default=10)

    return p.parse_args(argv)


def resolve_work_dir(args: argparse.Namespace) -> str:
    """Resolve the working directory from CLI arg, environment variable, or default.

    Priority: --config-dir > UFF_CONFIG_DIR env var > DEFAULTS['config_dir'].

    Args:
        args: Parsed argparse.Namespace.

    Returns:
        Resolved working directory path string.
    """
    return args.config_dir or os.environ.get("UFF_CONFIG_DIR") or DEFAULTS["config_dir"]


# --------------------- main ---------------------


def main(argv: Optional[List[str]] = None) -> int:
    """Entry point: parse arguments, refresh cache, query, and emit JSON to stdout.

    All user-facing output is written as a single JSON object to stdout.
    Debug/diagnostic messages go to stderr only.  This separation ensures the
    output is safe to pipe directly into Home Assistant's command_line sensor.

    Args:
        argv: Argument list; defaults to sys.argv[1:] if None.

    Returns:
        Exit code: 0 on success, 2 on error.
    """
    args = parse_args(argv or sys.argv[1:])

    # Activate debug output if requested (writes to stderr only)
    global DEBUG
    DEBUG = bool(getattr(args, "debug", False))

    work_dir = resolve_work_dir(args)
    paths = make_paths(work_dir)

    debug_print(f"work_dir={work_dir}")

    # Start from DEFAULTS then override with any values found in config.json
    cfg = dict(DEFAULTS)
    cfg.update(load_config_from_dir(work_dir))

    debug_print(
        f"Config loaded from {paths.config_file if paths.config_file.exists() else 'None'}"
    )

    # Resolve credentials: CLI flag > config.json > environment variable
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

    # Bail early if no credentials are available from any source
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

    # Convert miles to km if the user supplied --radius-miles instead of --radius-km
    radius_km = args.radius_km
    if radius_km is None and args.radius_miles is not None:
        radius_km = args.radius_miles * 1.609344

    # Refresh cache (stations + prices) under exclusive file lock
    try:
        state, stats = ensure_cache(
            paths=paths,
            client_id=client_id,
            client_secret=client_secret,
            full_refresh=args.full_refresh,
            prices_refresh=args.prices_refresh,
            stations_baseline_days=int(
                cfg.get("stations_baseline_days", DEFAULTS["stations_baseline_days"])
            ),
            stations_incremental_hours=int(
                cfg.get("stations_incremental_hours", DEFAULTS["stations_incremental_hours"])
            ),
            prices_baseline_days=int(
                cfg.get("prices_baseline_days", DEFAULTS["prices_baseline_days"])
            ),
            prices_incremental_hours=float(
                cfg.get("prices_incremental_hours", DEFAULTS["prices_incremental_hours"])
            ),
            batch_sleep_seconds=float(
                cfg.get("batch_sleep_seconds", DEFAULTS["batch_sleep_seconds"])
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

    # Begin assembling the JSON output object
    out = {"state": "ok"}

    # Include cache health unless suppressed by --no-health
    if args.health or not args.no_health:
        debug_print("Cache health: added")
        out["cache"] = stats
        out["generated_at"] = iso_utc(utc_now())

    # Health-only mode: emit stats and exit without running a query
    if args.health:
        debug_print("Cache health: no other output required")
        print(json.dumps(out, ensure_ascii=False))
        return 0

    # Collect explicit station IDs, stripping any empty strings
    station_ids = [s for s in (args.station_id or []) if s]

    # Compile all regex filter arguments into pattern lists
    re_name = compile_res(args.re_name)
    re_postcode = compile_res(args.re_postcode)
    re_town = compile_res(args.re_town)
    re_id = compile_res(args.re_id)
    re_brand = compile_res(args.re_brand)
    re_fuel = compile_res(args.re_fuel)

    # Execute the station query against the cached data
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

    # Append optional output sections based on suppression flags
    if not args.no_stations:
        debug_print("Stations: added")
        out["stations"] = stations_list

    if not args.no_best:
        debug_print("Best: added")
        out.update(analysis)

    # Emit the final JSON response to stdout
    print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())