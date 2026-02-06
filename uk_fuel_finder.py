#!/usr/bin/env python3
"""
UK Fuel Finder - Home Assistant Integration
============================================

Multi-sensor fuel price monitoring with intelligent caching and incremental updates.

CONFIGURATION
-------------
Edit DEFAULT_CONFIG in the source below, or create a config file with API credentials and cache settings:

{
  "client_id": "YOUR_CLIENT_ID",
  "client_secret": "YOUR_CLIENT_SECRET",
  "token_file": "/config/.storage/uk_fuel_finder/token.json",
  "state_file": "/config/.storage/uk_fuel_finder/state.json",
  "stations_baseline_days": 30,
  "stations_incremental_hours": 1,
  "prices_incremental_hours": 1
}

Default config location: /config/uk_fuel_finder_config.json

USAGE
-----
Required arguments: --lat, --lon, and one of --radius-miles or --radius-km
(Not required for --healthcheck or --station-id)

Basic usage:
  python3 uk_fuel_finder.py --lat 51.5074 --lon -0.1278 --radius-miles 10

Using kilometers:
  python3 uk_fuel_finder.py --lat 53.8008 --lon -1.5491 --radius-km 8

With debug output:
  python3 uk_fuel_finder.py --lat 55.9533 --lon -3.1883 --radius-miles 10 --debug

Force full refresh (invalidate cache):
  python3 uk_fuel_finder.py --lat 51.5074 --lon -0.1278 --radius-miles 10 --full-refresh

Custom config file:
  python3 uk_fuel_finder.py --lat 51.5074 --lon -0.1278 --radius-miles 10 --config /path/to/config.json

Lookup specific station(s) by ID:
  python3 uk_fuel_finder.py --station-id abc123def456
  python3 uk_fuel_finder.py --station-id abc123,def456,ghi789 --fuel-types E10,E5
  python3 uk_fuel_finder.py --station-id abc123 --lat 53.8008 --lon -1.5491  # With distance

Search for stations by name (requires location to limit results):
  python3 uk_fuel_finder.py --station-name "tesco" --lat 51.5074 --lon -0.1278 --radius-miles 10
  python3 uk_fuel_finder.py --station-name "shell" --lat 53.8008 --lon -1.5491 --radius-miles 5 --max-stations 10
  python3 uk_fuel_finder.py --station-name "^BP " --lat 51.5074 --lon -0.1278 --radius-km 8  # Regex: starts with "BP "

HOME ASSISTANT INTEGRATION
---------------------------
Add sensors to configuration.yaml:

command_line:
  - sensor:
      name: "Fuel Finder Leeds"
      command: "python3 /config/uk_fuel_finder.py --lat 53.8008 --lon -1.5491 --radius-miles 10"
      scan_interval: 3600
      value_template: "{{ value_json.state }}"
      json_attributes:
        - icon
        - best_e10
        - best_b7
        - stations
        - last_update

  - sensor:
      name: "Fuel Finder London"
      command: "python3 /config/uk_fuel_finder.py --lat 51.5074 --lon -0.1278 --radius-km 8"
      scan_interval: 3600
      value_template: "{{ value_json.state }}"
      json_attributes:
        - icon
        - best_e10
        - best_b7
        - stations
        - last_update

OUTPUT FORMAT
-------------
JSON object with:
  state: Number of nearby stations with price data
  attributes:
    best_e10: Cheapest E10 station {name, postcode, miles, price}
    best_b7: Cheapest B7 diesel station {name, postcode, miles, price}
    stations: Array sorted by distancep or price
    last_update: ISO timestamp
"""

import argparse
import json
import sys
import os
import requests
import time
import fcntl
from datetime import datetime, timedelta
from pathlib import Path
from math import radians, cos, sin, asin, sqrt

# =============================================================================
# DEFAULT CONFIGURATION
# =============================================================================
DEFAULT_CONFIG = {
    "client_id": "YOUR_CLIENT_ID",
    "client_secret": "YOUR_CLIENT_SECRET",
    "token_file": "/config/.storage/uk_fuel_finder/token.json",
    "state_file": "/config/.storage/uk_fuel_finder/state.json",
    "stations_baseline_days": 30,  # Full redownload every N days
    "stations_incremental_hours": 1,  # Incremental updates every N hours
    "prices_incremental_hours": 1,  # Price updates every N hours
}

CONFIG = DEFAULT_CONFIG.copy()

# Debug flag - set by command line argument
DEBUG = False


def debug_print(msg):
    """Print debug message to stderr if debug mode enabled"""
    if DEBUG:
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {msg}", file=sys.stderr)


# =============================================================================
# HTTP REQUEST CONFIGURATION - Retry logic for API reliability
# =============================================================================
DEFAULT_HTTP = {
    "timeout": 60,  # Request timeout in seconds (increased for slow API)
    "retries": 5,  # Total number of attempts (including first try)
    "backoff_base": 2.0,  # Exponential backoff multiplier
    "backoff_jitter": 1.0,  # Random jitter to avoid thundering herd (seconds)
}

# =============================================================================
# REQUEST WRAPPER WITH RETRY LOGIC
# =============================================================================


def request_with_retry(
    method,
    url,
    *,
    headers=None,
    params=None,
    json_body=None,
    timeout=DEFAULT_HTTP["timeout"],
    retries=DEFAULT_HTTP["retries"],
):
    """
    Retry on: 429, 500, 502, 503, 504, timeouts, connection resets.
    """
    from requests.exceptions import Timeout, ConnectionError, HTTPError, ReadTimeout

    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            debug_print(f"Request attempt {attempt}/{retries}: {method} {url}")

            resp = requests.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout,
            )

            # Retryable HTTP status codes
            if resp.status_code in (429, 500, 502, 503, 504):
                debug_print(f"Received retryable status {resp.status_code}")

                # honour Retry-After if present (mainly for 429)
                ra = resp.headers.get("Retry-After")
                if ra:
                    try:
                        sleep_s = float(ra)
                    except ValueError:
                        sleep_s = None
                else:
                    sleep_s = None

                if attempt < retries:
                    if sleep_s is None:
                        sleep_s = (
                            DEFAULT_HTTP["backoff_base"] ** (attempt - 1)
                        ) + DEFAULT_HTTP["backoff_jitter"]
                    debug_print(f"Retrying in {sleep_s:.1f}s...")
                    time.sleep(sleep_s)
                    continue
                else:
                    # Last attempt, let it raise
                    debug_print(
                        f"All retries exhausted, failing with status {resp.status_code}"
                    )
                    resp.raise_for_status()

            debug_print(f"Request successful: {resp.status_code}")
            return resp

        except (Timeout, ReadTimeout, ConnectionError, HTTPError) as e:
            last_exc = e
            debug_print(f"Request failed: {type(e).__name__}: {str(e)[:100]}")

            # If it's an HTTPError, only retry if retryable status
            if isinstance(e, HTTPError):
                status = getattr(e.response, "status_code", None)
                if status not in (429, 500, 502, 503, 504):
                    debug_print(f"Non-retryable status {status}, raising")
                    raise

            if attempt < retries:
                sleep_s = (
                    DEFAULT_HTTP["backoff_base"] ** (attempt - 1)
                ) + DEFAULT_HTTP["backoff_jitter"]
                debug_print(f"Retrying in {sleep_s:.1f}s...")
                time.sleep(sleep_s)
                continue

            debug_print(f"All retries exhausted")
            raise

    # should never reach
    raise last_exc


# =============================================================================
# CONFIG LOADING / CLI
# =============================================================================


def load_config_file(path_str: str) -> dict:
    """Load JSON config file."""
    p = Path(path_str)
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Config file must contain a JSON object")
    return data


def apply_overrides(cfg: dict, overrides: dict) -> dict:
    """Return cfg with non-None overrides applied."""
    out = cfg.copy()
    for k, v in overrides.items():
        if v is not None:
            out[k] = v
    return out


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="UK Fuel Finder - multi-sensor fuel price monitoring with shared cache.",
        epilog="""
Examples:
  %(prog)s --lat 51.5074 --lon -0.1278 --radius-miles 10
  %(prog)s --lat 53.8008 --lon -1.5491 --radius-km 8
  %(prog)s --lat 51.5074 --lon -0.1278 --radius-miles 10 --debug
  %(prog)s --lat 51.5074 --lon -0.1278 --radius-miles 10 --full-refresh
  %(prog)s --lat 51.5074 --lon -0.1278 --radius-miles 10 --fuel-types E10,E5,B7_STANDARD
  %(prog)s --lat 51.5074 --lon -0.1278 --radius-miles 10 --max-stations 5 --sort-by-price
  %(prog)s --healthcheck
  %(prog)s --station-id abc123def456
  %(prog)s --station-id abc123,def456 --lat 51.5074 --lon -0.1278
  %(prog)s --station-name "tesco" --lat 51.5074 --lon -0.1278 --radius-miles 10
  %(prog)s --station-name "^shell" --lat 53.8008 --lon -1.5491 --radius-km 8 --max-stations 5

Location (--lat, --lon, radius) required except for --healthcheck or --station-id.
Station name search (--station-name) ALWAYS requires location to prevent thousands of results.

For Home Assistant, run multiple instances with different coordinates:
  command: "python3 /config/uk_fuel_finder.py --lat 51.5074 --lon -0.1278 --radius-miles 10"
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Healthcheck mode (standalone)
    parser.add_argument(
        "--healthcheck",
        action="store_true",
        help="Check cache health and exit (exit 0 if valid, 1 if stale/missing)",
    )

    # Station ID lookup (standalone or with location)
    parser.add_argument(
        "--station-id",
        type=str,
        help="Get prices for specific station ID(s) (comma-separated)",
    )

    # Station name search (requires location for filtering)
    parser.add_argument(
        "--station-name",
        type=str,
        help="Search for stations by name (regex pattern supported, requires --lat/--lon/--radius)",
    )

    # Required location arguments (unless healthcheck or station-id only)
    location_group = parser.add_argument_group(
        "location (required except for --healthcheck or --station-id)"
    )
    location_group.add_argument("--lat", type=float, help="Latitude in decimal degrees")
    location_group.add_argument(
        "--lon", type=float, help="Longitude in decimal degrees"
    )

    # Radius - one required (unless healthcheck or station-id only)
    radius_group = parser.add_argument_group(
        "radius (choose one, required except for --healthcheck or --station-id)"
    )
    radius_exclusive = radius_group.add_mutually_exclusive_group()
    radius_exclusive.add_argument(
        "--radius-miles", type=float, help="Search radius in miles"
    )
    radius_exclusive.add_argument(
        "--radius-km", type=float, help="Search radius in kilometers"
    )

    # Output control
    output_group = parser.add_argument_group("output control")
    output_group.add_argument(
        "--fuel-types",
        type=str,
        default="E10,B7_STANDARD",
        help="Comma-separated fuel types to track (default: E10,B7_STANDARD). Available: E10, E5, B7_STANDARD, B7_PREMIUM, HVO, B10",
    )
    output_group.add_argument(
        "--max-stations",
        type=int,
        help="Maximum number of stations to return (default: all)",
    )
    output_group.add_argument(
        "--sort-by-price",
        action="store_true",
        help="Sort results by cheapest price instead of distance",
    )

    # Optional arguments
    parser.add_argument(
        "--config",
        metavar="PATH",
        default="/config/scripts/uk_fuel_finder_config.json",
        help="Path to JSON config file (default: /config/scripts/uk_fuel_finder_config.json)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Invalidate cache and force full baseline download of all data",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output to stderr showing progress and timing",
    )

    args = parser.parse_args(argv)

    # Validate: if not healthcheck or station-id, require location args
    # Note: station-name DOES require location (too many results without filtering)
    if not args.healthcheck and not args.station_id:
        if args.lat is None or args.lon is None:
            parser.error(
                "--lat and --lon are required (unless using --healthcheck or --station-id)"
            )
        if args.radius_miles is None and args.radius_km is None:
            parser.error(
                "one of --radius-miles or --radius-km is required (unless using --healthcheck or --station-id)"
            )

    return args


def normalise_config(cfg: dict) -> dict:
    """Normalise config types."""
    out = cfg.copy()
    for k in (
        "stations_baseline_days",
        "stations_incremental_hours",
        "prices_incremental_hours",
    ):
        if k in out and isinstance(out[k], str):
            try:
                out[k] = float(out[k])
            except ValueError:
                pass
    return out


# =============================================================================
# OAUTH TOKEN MANAGEMENT
# =============================================================================


def load_token():
    """Load cached OAuth token from file"""
    token_file = Path(CONFIG["token_file"])
    if token_file.exists():
        try:
            with token_file.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


def save_token(token_data):
    """Save OAuth token to file"""
    token_file = Path(CONFIG["token_file"])
    token_file.parent.mkdir(parents=True, exist_ok=True)
    with token_file.open("w", encoding="utf-8") as f:
        json.dump(token_data, f)


def get_token():
    """Get valid OAuth token, refreshing if necessary"""
    debug_print("Checking OAuth token...")
    cached = load_token()

    # Check if cached token is still valid
    if cached and cached.get("access_token") and cached.get("expires_at"):
        expires_at = datetime.fromisoformat(cached["expires_at"])
        if datetime.now() < expires_at - timedelta(seconds=30):
            debug_print("Using cached OAuth token")
            return cached["access_token"]

    # Need to get new token
    use_refresh = cached and cached.get("refresh_token")
    debug_print(f"Getting {'refresh' if use_refresh else 'new'} OAuth token...")

    if use_refresh:
        url = "https://www.fuel-finder.service.gov.uk/api/v1/oauth/regenerate_access_token"
        payload = {
            "client_id": CONFIG["client_id"],
            "refresh_token": cached["refresh_token"],
        }
    else:
        url = (
            "https://www.fuel-finder.service.gov.uk/api/v1/oauth/generate_access_token"
        )
        payload = {
            "client_id": CONFIG["client_id"],
            "client_secret": CONFIG["client_secret"],
        }

    response = requests.post(url, json=payload, headers={"accept": "application/json"})
    response.raise_for_status()

    data = response.json()
    token_data = data.get("data", data)

    access_token = token_data["access_token"]
    expires_in = token_data.get("expires_in", 3600)
    refresh_token = token_data.get(
        "refresh_token", cached.get("refresh_token") if cached else None
    )

    debug_print(f"OAuth token obtained, expires in {expires_in}s")

    # Save token
    save_token(
        {
            "access_token": access_token,
            "expires_at": (datetime.now() + timedelta(seconds=expires_in)).isoformat(),
            "refresh_token": refresh_token,
        }
    )

    return access_token


# =============================================================================
# API DATA FETCHING
# =============================================================================


def fetch_all_batches(token, path, params=None):
    """Fetch all paginated batches from API"""
    base_url = "https://www.fuel-finder.service.gov.uk"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {token}",
    }

    all_results = []
    batch = 1
    start_time = time.time()

    debug_print(f"Fetching batches from {path}...")

    while True:
        query_params = params.copy() if params else {}
        query_params["batch-number"] = batch

        response = request_with_retry(
            "GET",
            f"{base_url}{path}",
            headers=headers,
            params=query_params,
        )

        batch_data = response.json()
        if not isinstance(batch_data, list):
            batch_data = []

        all_results.extend(batch_data)
        debug_print(
            f"Batch {batch}: received {len(batch_data)} records (total: {len(all_results)})"
        )

        # If batch returned < 500 records, we're done
        if len(batch_data) < 500:
            break

        batch += 1

        # Rate limit: wait 1 second between batches to be polite to the API
        time.sleep(1)

    elapsed = time.time() - start_time
    debug_print(
        f"Fetched {len(all_results)} total records in {elapsed:.1f}s ({batch} batches)"
    )

    return all_results


# =============================================================================
# DISTANCE CALCULATION
# =============================================================================


def haversine_km(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in km"""
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return 6371 * c


# =============================================================================
# DATA PROCESSING
# =============================================================================


def get_opening_today(station):
    """Get today's opening hours"""
    days = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]
    day = days[datetime.now().weekday()]

    opening_times = station.get("opening_times", {}).get("usual_days", {}).get(day)
    if not opening_times:
        return None

    if opening_times.get("is_24_hours"):
        return "24h"

    open_time = (opening_times.get("open", "") or "")[:5]
    close_time = (opening_times.get("close", "") or "")[:5]

    if (
        not open_time
        or not close_time
        or (open_time == "00:00" and close_time == "00:00")
    ):
        return None

    return f"{open_time}-{close_time}"


def build_station_data(
    station_id, station, all_prices, fuel_list, home_lat=None, home_lon=None
):
    """
    Build consistent station data structure used across all query modes.

    Returns station dict with:
    - Basic info (id, name, brand, postcode, location)
    - Opening hours
    - Station type flags (motorway, supermarket)
    - Fuel prices for all requested types
    - Distance (if home location provided)
    """
    prices = all_prices.get(station_id, {})

    station_data = {
        "id": station_id,
        "name": station["name"],
        "brand": station["brand"],
        "postcode": station["postcode"],
        "lat": station["lat"],
        "lon": station["lon"],
        "open_today": get_opening_today(station),
        "is_motorway": station.get("is_mss", False),
        "is_supermarket": station.get("is_supermarket", False),
    }

    # Add distance if home location provided
    if home_lat is not None and home_lon is not None:
        km = haversine_km(home_lat, home_lon, station["lat"], station["lon"])
        station_data["distance_miles"] = round(km / 1.609344, 2)
        station_data["distance_km"] = round(km, 2)

    # Add prices for all requested fuel types
    for fuel_type in fuel_list:
        fuel_key = fuel_type.lower()
        fuel_data = prices.get(fuel_type, {})
        station_data[f"{fuel_key}_price"] = fuel_data.get("price")
        station_data[f"{fuel_key}_updated"] = fuel_data.get("timestamp")

    return station_data


def process_stations_to_dict(stations_data):
    """Convert station list to dict keyed by node_id"""
    debug_print(f"Processing {len(stations_data)} stations into dict...")
    stations_dict = {}

    for station in stations_data:
        node_id = station.get("node_id")
        if not node_id:
            continue

        location = station.get("location", {})
        lat = location.get("latitude")
        lon = location.get("longitude")

        if lat is None or lon is None:
            continue

        try:
            lat = float(lat)
            lon = float(lon)
        except (ValueError, TypeError):
            continue

        stations_dict[node_id] = {
            "id": node_id,
            "name": station.get("trading_name") or station.get("brand_name", ""),
            "brand": station.get("brand_name", ""),
            "postcode": location.get("postcode", ""),
            "lat": lat,
            "lon": lon,
            "temporary_closure": station.get("temporary_closure", False),
            "permanent_closure": station.get("permanent_closure", False),
            "opening_times": station.get("opening_times", {}),
            "is_mss": station.get("is_motorway_service_station", False),
            "is_supermarket": station.get("is_supermarket_service_station", False),
            "fuel_types": station.get("fuel_types", []),
            "amenities": station.get("amenities", []),
        }

    debug_print(f"Processed {len(stations_dict)} stations")
    return stations_dict


def filter_nearby_stations(all_stations, home_lat, home_lon, radius_km):
    """Filter stations dict to only those within radius"""
    debug_print(f"Filtering stations within {radius_km:.1f}km...")
    nearby = {}

    for node_id, station in all_stations.items():
        # Skip closed stations
        if station.get("temporary_closure") or station.get("permanent_closure"):
            continue

        km = haversine_km(home_lat, home_lon, station["lat"], station["lon"])
        if km <= radius_km:
            nearby[node_id] = station

    debug_print(f"Found {len(nearby)} stations within radius")
    return nearby


def process_prices_to_dict(prices_data):
    """Convert price list to dict keyed by node_id"""
    debug_print(f"Processing {len(prices_data)} price records...")
    prices_dict = {}
    max_timestamp = None

    for record in prices_data:
        node_id = record.get("node_id")
        if not node_id:
            continue

        fuel_prices = record.get("fuel_prices", [])
        station_prices = {}

        for fuel in fuel_prices:
            fuel_type = fuel.get("fuel_type")
            if not fuel_type:
                continue

            price = fuel.get("price")
            if price is None or price == "":
                continue

            try:
                price = float(price)
            except (ValueError, TypeError):
                continue

            # Fix data entry errors: 
            # if price < 2p, it's likely in pounds (multiply by 100)
            if price < 2.0:
                debug_print(f"Fixed price error: {price}p -> {price * 100}p for {fuel_type} at station {node_id[:8]}...")
                price = price * 100

            timestamp = fuel.get("price_last_updated")

            station_prices[fuel_type] = {"price": price, "timestamp": timestamp}

            if timestamp and (not max_timestamp or timestamp > max_timestamp):
                max_timestamp = timestamp

        if station_prices:
            prices_dict[node_id] = station_prices

    debug_print(f"Processed prices for {len(prices_dict)} stations")
    return prices_dict, max_timestamp


def merge_stations(base_dict, updates_dict):
    """Merge station updates into base dict"""
    debug_print(f"Merging {len(updates_dict)} station updates...")
    base_dict.update(updates_dict)
    debug_print(f"Total stations after merge: {len(base_dict)}")
    return base_dict


def merge_prices(base_dict, updates_dict):
    """Merge price updates into base dict"""
    debug_print(f"Merging {len(updates_dict)} price updates...")
    for node_id, fuels in updates_dict.items():
        if not isinstance(fuels, dict):
            continue
        existing = base_dict.get(node_id, {})
        if not isinstance(existing, dict):
            existing = {}
        existing.update(fuels)
        base_dict[node_id] = existing
    debug_print(f"Total stations with prices after merge: {len(base_dict)}")
    return base_dict


def build_output(
    nearby_stations,
    all_prices,
    fuel_types,
    max_stations=None,
    sort_by_price=False,
    mode="location",
    home_lat=None,
    home_lon=None,
    metadata=None,
    cache_state=None,
):
    """
    Build unified output JSON structure for all query modes.

    Args:
        nearby_stations: Dict of station_id -> station data
        all_prices: Dict of station_id -> fuel prices
        fuel_types: Comma-separated fuel type string
        max_stations: Optional limit on results
        sort_by_price: Sort by price instead of distance
        mode: Query mode ("location", "station_id", "station_name")
        home_lat: Home latitude for distance calculation
        home_lon: Home longitude for distance calculation
        metadata: Optional dict of mode-specific metadata
        cache_state: Optional cache state dict for timestamp info

    Returns:
        Unified JSON structure with consistent station data
    """
    debug_print("Building unified output...")
    result = []
    error_count = 0
    newest_price_timestamp = None

    # Parse fuel types (case-insensitive, convert to API format)
    fuel_list = []
    for f in fuel_types.split(","):
        f = f.strip().upper()
        # Normalize common variations
        if f == "B7":
            f = "B7_STANDARD"
        fuel_list.append(f)

    debug_print(f"Tracking fuel types: {fuel_list}")

    # Build station data for each nearby station
    for station_id, station in nearby_stations.items():
        # Check if this is an error entry (from station-id lookup)
        if isinstance(station, dict) and "error" in station:
            result.append(station)
            error_count += 1
            continue

        station_prices = all_prices.get(station_id, {})
        if not station_prices:
            continue

        # Use unified builder
        station_data = build_station_data(
            station_id, station, all_prices, fuel_list, home_lat, home_lon
        )

        # Track newest price timestamp
        for fuel_type in fuel_list:
            fuel_key = fuel_type.lower()
            timestamp = station_data.get(f"{fuel_key}_updated")
            if timestamp:
                if newest_price_timestamp is None or timestamp > newest_price_timestamp:
                    newest_price_timestamp = timestamp

        # Only include if station has at least one requested fuel type
        has_any_price = any(
            station_data.get(f"{fuel_type.lower()}_price") is not None
            for fuel_type in fuel_list
        )

        if has_any_price:
            result.append(station_data)

    found_count = len(result) - error_count

    # Sort results based on available criteria
    # Priority: --sort-by-price flag > location provided > alphabetical
    if sort_by_price:
        # Sort by cheapest available fuel (use first fuel type's price)
        first_fuel = fuel_list[0].lower()
        result = [s for s in result if not s.get("error")]  # Remove errors for sorting
        result.sort(
            key=lambda x: (
                x.get(f"{first_fuel}_price")
                if x.get(f"{first_fuel}_price") is not None
                else float("inf"),
                x.get("distance_miles", float("inf"))
                if home_lat and home_lon
                else x.get("name", ""),  # Secondary sort
            )
        )
        debug_print(f"Sorted by {first_fuel} price (cheapest first)")
    elif home_lat and home_lon:
        # Sort by distance (default for location-based queries)
        result = [s for s in result if not s.get("error")]  # Remove errors for sorting
        result.sort(key=lambda x: x.get("distance_miles", float("inf")))
        debug_print("Sorted by distance (nearest first)")
    else:
        # Sort by name (for station-id without location and no sort flag)
        result = [s for s in result if not s.get("error")]  # Remove errors for sorting
        result.sort(key=lambda x: x.get("name", ""))
        debug_print("Sorted by name (alphabetical)")

    # Limit results if requested
    if max_stations and max_stations > 0:
        result = result[:max_stations]
        debug_print(f"Limited to {max_stations} stations")

    # Find cheapest for each fuel type (only for stations with prices)
    best_prices = {}
    stations_with_prices = [s for s in result if not s.get("error")]

    for fuel_type in fuel_list:
        fuel_key = fuel_type.lower()
        cheapest = None

        for station in stations_with_prices:
            price = station.get(f"{fuel_key}_price")
            if price is not None:
                if cheapest is None or price < cheapest["price"]:
                    cheapest = {
                        "name": station["name"],
                        "postcode": station["postcode"],
                        "miles": station.get("distance_miles"),
                        "price": price,
                    }

        if cheapest:
            best_prices[f"best_{fuel_key}"] = cheapest
            debug_print(
                f"Cheapest {fuel_type}: {cheapest['price']}p at {cheapest['name']}"
            )

    # Build attributes with timestamps
    details = {
        "icon": "mdi:gas-station",
        **best_prices,
        **(metadata or {}),  # Add mode-specific metadata
    }

    # Add cache timestamp info if available
    if cache_state:
        details["cache_updated"] = cache_state.get("prices_last_incremental")
        details["cache_baseline"] = cache_state.get("stations_baseline_at")

    # Add newest price timestamp if found
    if newest_price_timestamp:
        details["newest_price"] = newest_price_timestamp

    # Build unified output structure
    output = {
        "state": len(result),
        "mode": mode,
        "found": found_count,
        "errors": error_count,
        "details": details,  # Renamed from 'attributes' to avoid HA reserved word
        "stations": result,
        "last_update": datetime.now().isoformat(),  # When script ran
    }

    return output


# =============================================================================
# STATE FILE MANAGEMENT WITH FILE LOCKING
# =============================================================================


def load_state():
    """Load state file with proper structure"""
    state_file = Path(CONFIG["state_file"])
    if state_file.exists():
        try:
            with state_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except Exception as e:
            debug_print(f"Failed to load state: {e}")

    # Return empty structure
    return {
        "all_uk_stations": {},
        "all_uk_prices": {},
        "stations_baseline_at": None,
        "stations_last_incremental": None,
        "prices_last_incremental": None,
    }


def save_state(state):
    """Save state to file"""
    state_file = Path(CONFIG["state_file"])
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with state_file.open("w", encoding="utf-8") as f:
        json.dump(state, f)


def acquire_lock(state_file):
    """Acquire exclusive lock on state file"""
    lock_file = Path(str(state_file) + ".lock")
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    lock_file.touch(exist_ok=True)

    lock_fd = open(lock_file, "w")
    debug_print("Acquiring file lock...")
    fcntl.flock(lock_fd, fcntl.LOCK_EX)
    debug_print("Lock acquired")
    return lock_fd


def release_lock(lock_fd):
    """Release file lock and clean up lock file"""
    debug_print("Releasing file lock...")
    lock_file_path = lock_fd.name
    fcntl.flock(lock_fd, fcntl.LOCK_UN)
    lock_fd.close()

    # Clean up lock file
    try:
        Path(lock_file_path).unlink()
    except Exception:
        pass  # Ignore if already deleted


def needs_baseline_refresh(state):
    """Check if baseline needs refresh"""
    baseline_at = state.get("stations_baseline_at")
    if not baseline_at:
        return True

    try:
        baseline_dt = datetime.fromisoformat(baseline_at)
        age_days = (datetime.now() - baseline_dt).total_seconds() / 86400
        baseline_days = float(CONFIG.get("stations_baseline_days", 30))
        return age_days >= baseline_days
    except Exception:
        return True


def needs_stations_incremental(state):
    """Check if stations need incremental update"""
    last_incremental = state.get("stations_last_incremental")
    if not last_incremental:
        return True

    try:
        last_dt = datetime.fromisoformat(last_incremental)
        age_hours = (datetime.now() - last_dt).total_seconds() / 3600
        incremental_hours = float(CONFIG.get("stations_incremental_hours", 1))
        return age_hours >= incremental_hours
    except Exception:
        return True


def needs_prices_incremental(state):
    """Check if prices need incremental update"""
    last_incremental = state.get("prices_last_incremental")
    if not last_incremental:
        return True

    try:
        last_dt = datetime.fromisoformat(last_incremental)
        age_hours = (datetime.now() - last_dt).total_seconds() / 3600
        incremental_hours = float(CONFIG.get("prices_incremental_hours", 1))
        return age_hours >= incremental_hours
    except Exception:
        return True


# =============================================================================
# CACHE UPDATE LOGIC
# =============================================================================


def update_cache(token, state, force_baseline=False):
    """Update shared cache with latest stations and prices"""
    updated = False

    # Check if baseline refresh needed
    if force_baseline or needs_baseline_refresh(state):
        debug_print("Performing baseline refresh (full UK download)...")

        # Download all UK stations
        stations_data = fetch_all_batches(token, "/api/v1/pfs")
        state["all_uk_stations"] = process_stations_to_dict(stations_data)
        state["stations_baseline_at"] = datetime.now().isoformat()
        state["stations_last_incremental"] = datetime.now().isoformat()

        # Download all UK prices
        prices_data = fetch_all_batches(token, "/api/v1/pfs/fuel-prices")
        prices_dict, max_timestamp = process_prices_to_dict(prices_data)
        state["all_uk_prices"] = prices_dict
        if max_timestamp:
            state["prices_last_incremental"] = max_timestamp

        updated = True
        debug_print("Baseline refresh complete")

    else:
        # Check for incremental stations update
        if needs_stations_incremental(state):
            debug_print("Performing incremental stations update...")
            last_incremental = state.get("stations_last_incremental")

            # Fetch incremental stations
            params = {}
            if last_incremental:
                dt = datetime.fromisoformat(last_incremental.replace("Z", "+00:00"))
                dt = dt - timedelta(minutes=30)  # Safety margin
                params["effective-start-timestamp"] = dt.strftime("%Y-%m-%d %H:%M:%S")

            stations_data = fetch_all_batches(token, "/api/v1/pfs", params)
            if stations_data:
                updates = process_stations_to_dict(stations_data)
                state["all_uk_stations"] = merge_stations(
                    state["all_uk_stations"], updates
                )
                updated = True

            state["stations_last_incremental"] = datetime.now().isoformat()
            debug_print("Incremental stations update complete")

        # Check for incremental prices update
        if needs_prices_incremental(state):
            debug_print("Performing incremental prices update...")
            last_incremental = state.get("prices_last_incremental")

            # Fetch incremental prices
            params = {}
            if last_incremental:
                dt = datetime.fromisoformat(last_incremental.replace("Z", "+00:00"))
                dt = dt - timedelta(minutes=30)  # Safety margin
                params["effective-start-timestamp"] = dt.strftime("%Y-%m-%d %H:%M:%S")

            prices_data = fetch_all_batches(token, "/api/v1/pfs/fuel-prices", params)
            if prices_data:
                updates, max_timestamp = process_prices_to_dict(prices_data)
                state["all_uk_prices"] = merge_prices(state["all_uk_prices"], updates)
                if max_timestamp:
                    state["prices_last_incremental"] = max_timestamp
                updated = True
            else:
                state["prices_last_incremental"] = datetime.now().isoformat()

            debug_print("Incremental prices update complete")

    return state, updated


# =============================================================================
# MAIN
# =============================================================================


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    args = parse_args(argv)

    # Set global debug flag
    global DEBUG
    DEBUG = args.debug

    # Load config (needed even for healthcheck)
    global CONFIG
    cfg = DEFAULT_CONFIG.copy()

    config_path = Path(args.config)
    if config_path.exists():
        if DEBUG:
            debug_print(f"Loading config from {args.config}")
        cfg = apply_overrides(cfg, load_config_file(args.config))
    elif args.config != "/config/scripts/uk_fuel_finder_config.json":
        # User specified a custom config file but it doesn't exist - error
        print(f"Error: Config file not found: {args.config}", file=sys.stderr)
        sys.exit(1)
    else:
        # Default config path doesn't exist - use hardcoded defaults
        if DEBUG:
            debug_print(
                f"Config file not found at default location, using hardcoded defaults"
            )

    CONFIG = normalise_config(cfg)

    # Healthcheck mode
    if args.healthcheck:
        if DEBUG:
            debug_print("=== Healthcheck Mode ===")

        try:
            state = load_state()

            # Check if cache exists and is valid
            if not state.get("all_uk_stations") or not state.get("all_uk_prices"):
                if DEBUG:
                    debug_print("FAIL: Cache is empty or missing")
                sys.exit(1)

            # Check baseline age
            baseline_at = state.get("stations_baseline_at")
            if not baseline_at:
                if DEBUG:
                    debug_print("FAIL: No baseline timestamp")
                sys.exit(1)

            # Parse timestamps (handle both timezone-aware and naive)
            now = datetime.now()

            baseline_dt = datetime.fromisoformat(baseline_at.replace("Z", "+00:00"))
            # Make naive for comparison (we only care about elapsed time)
            if baseline_dt.tzinfo is not None:
                baseline_dt = baseline_dt.replace(tzinfo=None)

            baseline_age_days = (now - baseline_dt).total_seconds() / 86400
            baseline_days_limit = float(CONFIG.get("stations_baseline_days", 30))

            # Check incremental ages
            stations_inc = state.get("stations_last_incremental")
            prices_inc = state.get("prices_last_incremental")

            stations_age_hours = 999
            prices_age_hours = 999

            if stations_inc:
                stations_dt = datetime.fromisoformat(
                    stations_inc.replace("Z", "+00:00")
                )
                if stations_dt.tzinfo is not None:
                    stations_dt = stations_dt.replace(tzinfo=None)
                stations_age_hours = (now - stations_dt).total_seconds() / 3600

            if prices_inc:
                prices_dt = datetime.fromisoformat(prices_inc.replace("Z", "+00:00"))
                if prices_dt.tzinfo is not None:
                    prices_dt = prices_dt.replace(tzinfo=None)
                prices_age_hours = (now - prices_dt).total_seconds() / 3600

            stations_inc_limit = float(CONFIG.get("stations_incremental_hours", 1))
            prices_inc_limit = float(CONFIG.get("prices_incremental_hours", 1))

            if DEBUG:
                debug_print(
                    f"Baseline age: {baseline_age_days:.1f} days (limit: {baseline_days_limit})"
                )
                debug_print(
                    f"Stations incremental age: {stations_age_hours:.1f} hours (limit: {stations_inc_limit})"
                )
                debug_print(
                    f"Prices incremental age: {prices_age_hours:.1f} hours (limit: {prices_inc_limit})"
                )
                debug_print(f"Total stations: {len(state.get('all_uk_stations', {}))}")
                debug_print(
                    f"Stations with prices: {len(state.get('all_uk_prices', {}))}"
                )

            # Determine health
            is_healthy = (
                baseline_age_days < baseline_days_limit
                and stations_age_hours < stations_inc_limit * 2  # Allow 2x grace period
                and prices_age_hours < prices_inc_limit * 2
            )

            if is_healthy:
                if DEBUG:
                    debug_print("PASS: Cache is healthy")
                print(
                    json.dumps(
                        {
                            "healthy": True,
                            "baseline_age_days": round(baseline_age_days, 1),
                            "stations_age_hours": round(stations_age_hours, 1),
                            "prices_age_hours": round(prices_age_hours, 1),
                            "total_stations": len(state.get("all_uk_stations", {})),
                            "stations_with_prices": len(state.get("all_uk_prices", {})),
                        }
                    )
                )
                sys.exit(0)
            else:
                if DEBUG:
                    debug_print("FAIL: Cache is stale")
                print(
                    json.dumps(
                        {
                            "healthy": False,
                            "baseline_age_days": round(baseline_age_days, 1),
                            "stations_age_hours": round(stations_age_hours, 1),
                            "prices_age_hours": round(prices_age_hours, 1),
                            "reason": "Cache is stale",
                        }
                    )
                )
                sys.exit(1)

        except Exception as e:
            if DEBUG:
                import traceback

                debug_print(f"FAIL: {e}")
                debug_print(traceback.format_exc())
            print(json.dumps({"healthy": False, "error": str(e)}))
            sys.exit(1)

    # Station ID lookup mode
    if args.station_id:
        if DEBUG:
            debug_print("=== Station ID Lookup Mode ===")

        try:
            # Parse station IDs
            station_ids = [s.strip() for s in args.station_id.split(",")]
            if DEBUG:
                debug_print(f"Looking up {len(station_ids)} station(s)")

            # Load cache
            state = load_state()
            if not state.get("all_uk_stations") or not state.get("all_uk_prices"):
                if DEBUG:
                    debug_print("Cache is empty, populating from API...")

                # Get OAuth token
                token = get_token()

                # Acquire lock and populate cache
                state_file = Path(CONFIG["state_file"])
                lock_fd = acquire_lock(state_file)

                try:
                    # Force baseline refresh
                    state, _ = update_cache(token, state, force_baseline=True)
                    save_state(state)
                    if DEBUG:
                        debug_print("Cache populated successfully")
                finally:
                    release_lock(lock_fd)

            # Build stations dict (with errors for not-found IDs)
            stations_dict = {}
            for station_id in station_ids:
                station = state["all_uk_stations"].get(station_id)
                if not station:
                    # Add error entry
                    stations_dict[station_id] = {
                        "id": station_id,
                        "error": "Station not found in cache",
                    }
                else:
                    stations_dict[station_id] = station

            # Build unified output
            output = build_output(
                stations_dict,
                state["all_uk_prices"],
                args.fuel_types,
                max_stations=args.max_stations,
                sort_by_price=args.sort_by_price,
                mode="station_id",
                home_lat=args.lat,
                home_lon=args.lon,
                metadata={},
                cache_state=state,
            )

            if DEBUG:
                debug_print(
                    f"Found {output['found']} station(s), {output['errors']} error(s)"
                )

            print(json.dumps(output))
            sys.exit(0)

        except Exception as e:
            if DEBUG:
                import traceback

                debug_print(f"FAIL: {e}")
                debug_print(traceback.format_exc())
            print(json.dumps({"error": str(e)}), file=sys.stderr)
            sys.exit(1)

    # Station name search mode
    if args.station_name:
        if DEBUG:
            debug_print("=== Station Name Search Mode ===")

        try:
            search_pattern = args.station_name.strip()
            if DEBUG:
                debug_print(
                    f"Searching for stations matching pattern: '{search_pattern}'"
                )

            # Load cache
            state = load_state()
            if not state.get("all_uk_stations") or not state.get("all_uk_prices"):
                if DEBUG:
                    debug_print("Cache is empty, populating from API...")

                # Get OAuth token
                token = get_token()

                # Acquire lock and populate cache
                state_file = Path(CONFIG["state_file"])
                lock_fd = acquire_lock(state_file)

                try:
                    # Force baseline refresh
                    state, _ = update_cache(token, state, force_baseline=True)
                    save_state(state)
                    if DEBUG:
                        debug_print("Cache populated successfully")
                finally:
                    release_lock(lock_fd)

            # Calculate radius in km (required for station-name)
            if args.radius_miles:
                radius_km = args.radius_miles * 1.609344
                radius_display = f"{args.radius_miles}mi"
            else:
                radius_km = args.radius_km
                radius_display = f"{args.radius_km}km"

            # First, filter to nearby stations (within radius)
            if DEBUG:
                debug_print(f"Filtering to stations within {radius_km:.1f}km...")

            nearby_stations = filter_nearby_stations(
                state["all_uk_stations"], args.lat, args.lon, radius_km
            )

            # Compile regex pattern (case-insensitive)
            import re

            try:
                pattern = re.compile(search_pattern, re.IGNORECASE)
            except re.error as e:
                print(
                    json.dumps({"error": f"Invalid regex pattern: {e}"}),
                    file=sys.stderr,
                )
                sys.exit(1)

            # Search within nearby stations for matching name/brand/postcode
            matching_stations = {}
            for station_id, station in nearby_stations.items():
                # Search in name, brand, and postcode
                station_name = station.get("name") or ""
                station_brand = station.get("brand") or ""
                station_postcode = station.get("postcode") or ""

                if (
                    pattern.search(station_name)
                    or pattern.search(station_brand)
                    or pattern.search(station_postcode)
                ):
                    matching_stations[station_id] = station

            if DEBUG:
                debug_print(f"Found {len(matching_stations)} matching station(s)")

            # Build unified output with metadata
            metadata = {
                "search_pattern": args.station_name,
                "radius_miles": round(radius_km / 1.609344, 1)
                if args.radius_miles
                else None,
                "radius_km": round(radius_km, 1) if args.radius_km else None,
            }

            output = build_output(
                matching_stations,
                state["all_uk_prices"],
                args.fuel_types,
                max_stations=args.max_stations,
                sort_by_price=args.sort_by_price,
                mode="station_name",
                home_lat=args.lat,
                home_lon=args.lon,
                metadata=metadata,
                cache_state=state,
            )

            print(json.dumps(output))
            sys.exit(0)

        except Exception as e:
            if DEBUG:
                import traceback

                debug_print(f"FAIL: {e}")
                debug_print(traceback.format_exc())
            print(json.dumps({"error": str(e)}), file=sys.stderr)
            sys.exit(1)

    # Normal mode - require location arguments
    if DEBUG:
        debug_print("=== UK Fuel Finder ===")
        debug_print(f"Started at {datetime.now().isoformat()}")

    # Calculate radius in km
    if args.radius_miles:
        radius_km = args.radius_miles * 1.609344
        radius_display = f"{args.radius_miles}mi"
    else:
        radius_km = args.radius_km
        radius_display = f"{args.radius_km}km"

    if DEBUG:
        debug_print(
            f"Location: ({args.lat:.4f}, {args.lon:.4f}), radius={radius_display}"
        )
        debug_print(f"Fuel types: {args.fuel_types}")
        if args.max_stations:
            debug_print(f"Max stations: {args.max_stations}")
        if args.sort_by_price:
            debug_print("Sort by: price (cheapest first)")
        else:
            debug_print("Sort by: distance (nearest first)")

    try:
        start_time = time.time()

        # Get OAuth token
        token = get_token()

        # Acquire file lock for cache access
        state_file = Path(CONFIG["state_file"])
        lock_fd = acquire_lock(state_file)

        try:
            # Load current state
            debug_print("Loading state file...")
            state = load_state()

            # Check if full refresh requested
            if args.full_refresh:
                debug_print("Full refresh requested - invalidating cache")
                state = {
                    "all_uk_stations": {},
                    "all_uk_prices": {},
                    "stations_baseline_at": None,
                    "stations_last_incremental": None,
                    "prices_last_incremental": None,
                }

            # Update cache if needed
            state, cache_updated = update_cache(
                token, state, force_baseline=args.full_refresh
            )

            # Save state if updated
            if cache_updated:
                debug_print("Saving updated state...")
                save_state(state)
            else:
                debug_print("Cache is fresh, no update needed")

        finally:
            # Always release lock
            release_lock(lock_fd)

        # Filter to nearby stations
        nearby_stations = filter_nearby_stations(
            state["all_uk_stations"], args.lat, args.lon, radius_km
        )

        # Build unified output
        metadata = {
            "radius_miles": round(radius_km / 1.609344, 1)
            if args.radius_miles
            else None,
            "radius_km": round(radius_km, 1) if args.radius_km else None,
        }

        output = build_output(
            nearby_stations,
            state["all_uk_prices"],
            args.fuel_types,
            args.max_stations,
            args.sort_by_price,
            mode="location",
            home_lat=args.lat,
            home_lon=args.lon,
            metadata=metadata,
            cache_state=state,
        )

        elapsed = time.time() - start_time
        debug_print(
            f"=== Complete in {elapsed:.1f}s - found {output['found']} stations with prices ==="
        )

        # Output JSON to stdout
        print(json.dumps(output))

    except Exception as e:
        import traceback

        debug_print(f"FATAL ERROR: {e}")
        print(
            json.dumps(
                {
                    "state": "error",
                    "attributes": {
                        "error": str(e),
                        "traceback": traceback.format_exc(),
                    },
                }
            ),
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
