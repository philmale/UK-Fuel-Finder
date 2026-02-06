# UK Fuel Finder - Home Assistant Integration

Multi-sensor fuel price monitoring with intelligent caching and incremental updates for the UK Government Fuel Finder API.

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)

## Features

- **Smart Caching** - Download UK stations once, share across all queries
- **Incremental Updates** - Monthly baseline + hourly incremental updates
- **Multi-Sensor** - Run multiple instances for different locations
- **Thread-Safe** - File locking for concurrent execution
- **Flexible Fuel Types** - Track E10, E5, B7, B7 Premium, HVO, B10
- **Customizable Output** - Sort by price or distance, limit results
- **Health Check** - Monitor cache validity for automation
- **Debug Mode** - Comprehensive progress tracking

## Quick Start

### 1. Get API Credentials

Register for free at [UK Government Fuel Finder](https://www.fuel-finder.service.gov.uk/) to get your `client_id` and `client_secret`.

### 2. Install

For Home Assistant copy uk_fuekl_finder.py into `/config` or somewhere below, I prefer `/config/scripts`.

### 3. Configure

You can set the defaults by editing the `DEFAULT_CONFIG` block in the script itself, or you can create `/config/scripts/uk_fuel_finder_config.json`:

```json
{
  "client_id": "your_client_id_here",
  "client_secret": "your_client_secret_here",
  "token_file": "/config/.storage/uk_fuel_finder/token.json",
  "state_file": "/config/.storage/uk_fuel_finder/state.json",
  "stations_baseline_days": 30,
  "stations_incremental_hours": 1,
  "prices_incremental_hours": 1
}
```

All config options can also be overridden on the command line itself.

### 4. Test

If you are using this for Home Assistant and running Home Assistant in a Docker container, run the test from
inside the container:

```bash
docker exec -ti homeassistant /bin/bash
cd /config/scripts
```

If you are running Home Assistant any other way, or using this for another application, then just run it from
thew command line (after checking you have setup the coniguration correctly so you know which directories the
data will be saved in).

```bash
cd /config/scripts
python3 uk_fuel_finder.py --lat 51.5074 --lon -0.1278 --radius-miles 10 --debug
```

If you have `jq` available (it is inside the Home Assistant environment) then you can run this test:
```bash
cd /config/scripts
python3 uk_fuel_finder.py --lat 51.5074 --lon -0.1278 --radius-miles 10 --debug | jq
```

First run takes ~3 minutes (downloads all UK stations and sets up the state files). Subsequent runs take <1 second.

### 5. Add to Home Assistant

Add to `configuration.yaml` or an included (`command_line: !include command_line.yaml` for example) sensor file:

```yaml
command_line:
  - sensor:
      name: "Fuel Finder Home"
      command: "python3 /config/scripts/uk_fuel_finder.py --lat 53.8345 --lon -1.5435 --radius-miles 10"
      scan_interval: 3600
      icon: "mdi:gas-station"
      value_template: "{{ value_json.state }}"
      json_attributes:
        - mode
        - found
        - errors
        - details
        - stations
        - last_update
        
  - sensor:
      name: "Fuel Finder Work"
      command: "python3 /config/scripts/uk_fuel_finder.py --lat 53.8008 --lon -1.5491 --radius-km 8"
      scan_interval: 3600
      icon: "mdi:gas-station"
      value_template: "{{ value_json.state }}"
      json_attributes:
        - mode
        - found
        - errors
        - details
        - stations
        - last_update
```

This example creates two sensors in Home Assistant. You can change the command line to obtain the output you want.

For reference, here is the one I use (the markdown lovelace card I use to display is show below):

```yaml
# Petrol Prices
- sensor:
    name: "Local Petrol Prices"
    unique_id: local_petrol_prices
    scan_interval: 3600
    command_timeout: 900
    command: "python3 /config/scripts/uk_fuel_finder.py  --lat 53.8008 --lon -1.5491 --radius-miles 8 --fuel-types e10"
    value_template: "{{ value_json.state }}"
    icon: "{{ value_json.details.icon }}"
    state_class: measurement
    json_attributes:
      - mode
      - found
      - errors
      - details
      - stations
      - last_update
```

Restart Home Assistant or reload `YAML configuration` if you already had `command_line` sensors defined.

## Usage

You can run the python script in a lot of different ways, and that will depend upon the type of information you want to display.
Here are some examples but it really is easier to experiment from the command line to see what might be useful for you.

### Basic Commands

```bash
# Required arguments (London example)
python3 uk_fuel_finder.py --lat 51.5074 --lon -0.1278 --radius-miles 10

# Using kilometers (Leeds example)
python3 uk_fuel_finder.py --lat 53.8008 --lon -1.5491 --radius-km 8

# With debug output (Edinburgh example)
python3 uk_fuel_finder.py --lat 55.9533 --lon -3.1883 --radius-miles 10 --debug
```

### Advanced Options

```bash
# Track specific fuel types (London)
python3 uk_fuel_finder.py --lat 51.5074 --lon -0.1278 --radius-miles 10 \
  --fuel-types E5,B7_PREMIUM

# Show 5 nearest stations (Leeds)
python3 uk_fuel_finder.py --lat 53.8008 --lon -1.5491 --radius-miles 10 \
  --max-stations 5

# Sort by price (cheapest first)
python3 uk_fuel_finder.py --lat 51.5074 --lon -0.1278 --radius-miles 10 \
  --sort-by-price

# Show 3 cheapest E10 stations
python3 uk_fuel_finder.py --lat 51.5074 --lon -0.1278 --radius-miles 20 \
  --fuel-types E10 --max-stations 3 --sort-by-price

# Force full cache refresh
python3 uk_fuel_finder.py --lat 51.5074 --lon -0.1278 --radius-miles 10 \
  --full-refresh

# Check cache health
python3 uk_fuel_finder.py --healthcheck

# Lookup specific station(s) by ID
python3 uk_fuel_finder.py --station-id abc123def456
python3 uk_fuel_finder.py --station-id abc123,def456,ghi789 --fuel-types E10,E5

# Lookup station with distance calculation
python3 uk_fuel_finder.py --station-id abc123 --lat 51.5074 --lon -0.1278

# Search for stations by name (requires location to limit results)
python3 uk_fuel_finder.py --station-name "tesco" --lat 51.5074 --lon -0.1278 --radius-miles 10
python3 uk_fuel_finder.py --station-name "shell" --lat 53.8008 --lon -1.5491 --radius-miles 5 --max-stations 10
python3 uk_fuel_finder.py --station-name "^BP " --lat 51.5074 --lon -0.1278 --radius-km 8  # Regex: starts with "BP "
python3 uk_fuel_finder.py --station-name "sainsbury|tesco" --lat 55.9533 --lon -3.1883 --radius-miles 10  # Regex: OR
```

### Command-Line Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `--lat` | float | Yes* | Latitude in decimal degrees |
| `--lon` | float | Yes* | Longitude in decimal degrees |
| `--radius-miles` | float | Yes** | Search radius in miles |
| `--radius-km` | float | Yes** | Search radius in kilometers |
| `--fuel-types` | string | No | Comma-separated fuel types (default: E10,B7_STANDARD) |
| `--max-stations` | int | No | Limit number of results |
| `--sort-by-price` | flag | No | Sort by price instead of distance |
| `--station-id` | string | No | Lookup specific station ID(s), comma-separated |
| `--station-name` | string | No | Search stations by name/brand/postcode (regex supported, requires --lat/--lon/--radius) |
| `--config` | path | No | Config file path (default: /config/scripts/uk_fuel_finder_config.json) |
| `--full-refresh` | flag | No | Invalidate cache and redownload all data |
| `--healthcheck` | flag | No | Check cache health and exit |
| `--debug` | flag | No | Enable debug output to stderr |

\* Not required for `--healthcheck` or `--station-id`  
** Choose one: `--radius-miles` or `--radius-km` (not required for `--healthcheck` or `--station-id`)

**Note:** `--station-name` ALWAYS requires location parameters to prevent returning thousands of results.

### Available Fuel Types

- `E10` - Unleaded petrol with up to 10% ethanol
- `E5` - Unleaded petrol with up to 5% ethanol (premium)
- `B7_STANDARD` - Diesel with up to 7% biodiesel
- `B7_PREMIUM` - Premium diesel with up to 7% biodiesel
- `B10` - Diesel with up to 10% biodiesel
- `HVO` - Hydrotreated Vegetable Oil (renewable diesel)

## How It Works

### Shared Cache Architecture

```
First sensor run (e.g., 09:00):
  ├─ Updates cache (~2s for incremental, ~170s for baseline)
  ├─ Filters to nearby stations
  └─ Returns results

Second sensor run (e.g., 09:00):
  ├─ Cache is fresh (skip update)
  ├─ Filters to different location
  └─ Returns results (instant)

Third sensor run (e.g., 10:00):
  ├─ Updates cache (~2s incremental)
  ├─ Filters to nearby stations
  └─ Returns results
```

### Update Strategy

- **Monthly Baseline** (default: 30 days)
  - Downloads all ~7,000 UK stations
  - Takes ~3 minutes
  - Catches new station openings and closures

- **Hourly Incremental** (default: 1 hour)
  - Downloads only changed stations/prices
  - Takes ~2 seconds
  - Keeps data fresh

- **File Locking**
  - Uses `fcntl` to prevent race conditions
  - Multiple sensors can run simultaneously
  - Automatic lock cleanup

### Cache Structure

```json
{
  "all_uk_stations": {
    "station_id": {
      "name": "Station Name",
      "lat": 51.5074,
      "lon": -0.1278,
      ...
    }
  },
  "all_uk_prices": {
    "station_id": {
      "E10": {"price": 142.9, "timestamp": "2026-02-04T12:00:00Z"},
      "B7_STANDARD": {"price": 149.9, "timestamp": "2026-02-04T12:00:00Z"}
    }
  },
  "stations_baseline_at": "2026-02-04T03:00:00",
  "stations_last_incremental": "2026-02-04T23:00:00",
  "prices_last_incremental": "2026-02-04T23:00:00"
}
```

## Output Format

All modes return a unified JSON structure:

```json
{
  "state": 15,
  "mode": "location",
  "found": 15,
  "errors": 0,
  "details": {
    "icon": "mdi:gas-station",
    "best_e10": {
      "name": "Tesco Anytown",
      "postcode": "AB12 3CD",
      "miles": 2.3,
      "price": 142.9
    },
    "best_b7_standard": {
      "name": "Shell Anytown",
      "postcode": "AB12 4EF",
      "miles": 1.8,
      "price": 149.9
    },
    "radius_miles": 10.0,
    "radius_km": 16.1,
    "cache_updated": "2026-02-05T10:00:00.000Z",
    "cache_baseline": "2026-02-05T03:00:00",
    "newest_price": "2026-02-05T09:45:23.000Z"
  },
  "stations": [
    {
      "id": "station_id",
      "name": "Tesco Anytown",
      "brand": "TESCO",
      "postcode": "AB12 3CD",
      "lat": 51.5074,
      "lon": -0.1278,
      "distance_miles": 2.3,
      "distance_km": 3.7,
      "open_today": "06:00-22:00",
      "is_motorway": false,
      "is_supermarket": true,
      "e10_price": 142.9,
      "e10_updated": "2026-02-04T10:30:00Z",
      "b7_standard_price": 150.2,
      "b7_standard_updated": "2026-02-04T10:30:00Z"
    }
  ],
  "last_update": "2026-02-04T14:25:00"
}
```

### Top-Level Fields

| Field | Type | Description |
|-------|------|-------------|
| `state` | int | Number of stations returned |
| `mode` | string | Query mode: `location`, `station_id`, or `station_name` |
| `found` | int | Number of valid stations found |
| `errors` | int | Number of errors (e.g., station IDs not found) |
| `details` | object | Metadata including best prices, timestamps, search params |
| `stations` | array | Array of station objects with prices |
| `last_update` | string | ISO timestamp when script executed |

### Details Object

| Field | Type | Always Present? | Description |
|-------|------|-----------------|-------------|
| `icon` | string | Yes | Material Design icon for Home Assistant |
| `best_<fuel>` | object | If fuel found | Cheapest station for each fuel type requested |
| `radius_miles` | float | If applicable | Search radius in miles |
| `radius_km` | float | If applicable | Search radius in kilometers |
| `search_pattern` | string | station_name mode | Regex pattern used for search |
| `cache_updated` | string | Yes | When prices were last fetched from API |
| `cache_baseline` | string | Yes | When full station list was last downloaded |
| `newest_price` | string | If prices found | Most recent price timestamp in results |

### Station Object

All stations include these fields:

| Field | Type | Always Present? | Description |
|-------|------|-----------------|-------------|
| `id` | string | Yes | Unique station identifier (hash) |
| `name` | string | Yes | Station name |
| `brand` | string | Yes | Brand name (e.g., "SHELL", "TESCO") |
| `postcode` | string | Yes | UK postcode |
| `lat` | float | Yes | Latitude |
| `lon` | float | Yes | Longitude |
| `distance_miles` | float | If location provided | Distance in miles from search location |
| `distance_km` | float | If location provided | Distance in kilometers from search location |
| `open_today` | string | If available | Today's opening hours (e.g., "06:00-22:00", "24h") |
| `is_motorway` | boolean | Yes | True if motorway service station |
| `is_supermarket` | boolean | Yes | True if supermarket forecourt |
| `<fuel>_price` | float | If available | Price in pence per litre for requested fuel type |
| `<fuel>_updated` | string | If available | ISO timestamp when this fuel price was last updated |

### Timestamps Explained

| Timestamp | Location | Meaning |
|-----------|----------|---------|
| `last_update` | Top level | When this script execution completed |
| `cache_updated` | details | When prices were last fetched from API (hourly incremental) |
| `cache_baseline` | details | When full station list was last downloaded (monthly) |
| `newest_price` | details | Most recent price update timestamp across all returned stations |

  > All prices are in pence per litre (142.9p = £1.429/litre) - but this is AS REPORTED by the station.

## Home Assistant Dashboard

### Simple Markdown Card

A generic example using markdown:

```yaml
type: markdown
content: |
  ## Fuel Prices Near Home
  
  **Best E10:** {{ state_attr('sensor.fuel_finder_home', 'details').best_e10.name }}
  {{ state_attr('sensor.fuel_finder_home', 'details').best_e10.price }}p @ {{ state_attr('sensor.fuel_finder_home', 'details').best_e10.miles }} miles
  
  **Best Diesel:** {{ state_attr('sensor.fuel_finder_home', 'details').best_b7_standard.name }}
  {{ state_attr('sensor.fuel_finder_home', 'details').best_b7_standard.price }}p @ {{ state_attr('sensor.fuel_finder_home', 'details').best_b7_standard.miles }} miles
  
  **Last updated:** {{ relative_time(strptime(state_attr('sensor.fuel_finder_home', 'last_update'), '%Y-%m-%dT%H:%M:%S.%f')) }}
  
  ### Nearest 5 Stations
  {% for station in state_attr('sensor.fuel_finder_home', 'stations')[:5] %}
  **{{ station.name }}** ({{ station.distance_miles }} mi)
  E10: {{ station.e10_price }}p | Diesel: {{ station.b7_standard_price }}p
  Open: {{ station.open_today or 'Unknown' }}
  {% endfor %}
```
Here is a more advanced markdown display (actually the one I use which provides links to Waze for navigation from the Postcode display, the cardmod on the end is to remove grid lines in the display):
```yaml
  - type: markdown
    title: Local Fuel Prices
    content: >-
      {%- if states('sensor.local_petrol_prices') | int | default(0, true) >= 1
      -%}
        <center><table width="100%">
        {%- for station in
             state_attr('sensor.local_petrol_prices', 'stations')
             | selectattr('e10_price', 'defined')
             | sort(attribute='e10_price')
        -%}
          <tr>
            <td>{{ station.brand.split(' ')[0] | title }}, <font size=2>{{ station.name | title }}</font></td>
            <td><a href="https://waze.com/ul?ll={{ station.lat }}%2C{{ station.lon }}&navigate=yes&zoom=17">{{ station.postcode | upper }}</a></td>
            <td align="right">{{ station.e10_price }}p</td>
          </tr>
          <tr>
            <td colspan="3"><font size=1 color="grey">Open: {{ station.open_today }}, Dist: {{ station.distance_miles }} miles, Updated: {{ station.e10_updated | as_timestamp | timestamp_custom('%a %-d %b %Y %H:%M', true, 0)}}</font></td>
          </tr>
        {%- endfor -%}
        </table></center>
      {%- else -%}
        <center>No Fuel Prices Available</center>
      {%- endif -%}
    card_mod:
      style:
        ha-markdown $:
          ha-markdown-element: |
            td {
              border: none !important;
              padding: 0px !important;
            }
```

### Custom Button Card

```yaml
type: custom:button-carde
entity: sensor.fuel_finder_home
name: Cheapest E10
show_state: false
icon: mdi:gas-station
label: |
  [[[
    const best = entity.attributes.details.best_e10;
    return `${best.price}p @ ${best.name} (${best.miles} mi)`;
  ]]]
styles:
  card:
    - height: 80px
  label:
    - font-size: 12px
```

### Template Sensor Examples

```yaml
template:
  - sensor:
      - name: "Cheapest E10 Price"
        unit_of_measurement: "p"
        state: "{{ state_attr('sensor.fuel_finder_home', 'details').best_e10.price }}"
        
      - name: "Cheapest E10 Station"
        state: "{{ state_attr('sensor.fuel_finder_home', 'details').best_e10.name }}"
        
      - name: "Fuel Data Age"
        state: >
          {{ relative_time(strptime(state_attr('sensor.fuel_finder_home', 'details').cache_updated, '%Y-%m-%dT%H:%M:%S.%fZ')) }}
```

## Health Check Mode

Use `--healthcheck` for monitoring and automation:

```bash
# Check cache health
python3 uk_fuel_finder.py --healthcheck

# Exit codes:
#   0 = Cache is healthy and fresh
#   1 = Cache is stale, missing, or invalid

# Example output (healthy):
{
  "healthy": true,
  "baseline_age_days": 15.2,
  "stations_age_hours": 0.5,
  "prices_age_hours": 0.3,
  "total_stations": 6794,
  "stations_with_prices": 6532
}

# Use in scripts
if python3 uk_fuel_finder.py --healthcheck; then
  echo "Cache is healthy"
else
  echo "Cache needs refresh"
  python3 uk_fuel_finder.py --lat 51.5074 --lon -0.1278 --radius-miles 10 --full-refresh
fi
```

## Station ID Lookup

Query specific stations by their unique ID:

```bash
# Single station
python3 uk_fuel_finder.py --station-id abc123def456

# Multiple stations
python3 uk_fuel_finder.py --station-id abc123,def456,ghi789

# With specific fuel types
python3 uk_fuel_finder.py --station-id abc123 --fuel-types E5,B7_PREMIUM

# With distance calculation (provide your location)
python3 uk_fuel_finder.py --station-id abc123 --lat 51.5074 --lon -0.1278

# Example output:
{
  "state": 3,
  "mode": "station_id",
  "found": 2,
  "errors": 1,
  "details": {
    "icon": "mdi:gas-station",
    "best_e10": {
      "name": "Shell Example Station",
      "postcode": "SW1A 1AA",
      "miles": 2.3,
      "price": 142.9
    },
    "cache_updated": "2026-02-05T10:00:00.000Z",
    "newest_price": "2026-02-05T09:45:00.000Z"
  },
  "stations": [
    {
      "id": "abc123",
      "name": "Shell Example Station",
      "brand": "Shell",
      "postcode": "SW1A 1AA",
      "lat": 51.5074,
      "lon": -0.1278,
      "open_today": "06:00-22:00",
      "is_motorway": false,
      "is_supermarket": false,
      "e10_price": 142.9,
      "e10_updated": "2026-02-04T15:30:00Z",
      "distance_miles": 2.3,
      "distance_km": 3.7
    },
    {
      "id": "invalid_id",
      "error": "Station not found in cache"
    },
    {
      "id": "def456",
      "name": "Tesco Example",
      ...
    }
  ],
  "last_update": "2026-02-05T10:30:00"
}
```

**Features:**
- `state` - Total entries returned
- `found` - Number of successful lookups
- `errors` - Number of stations not found
- Optional distance calculation (provide `--lat`/`--lon`)
- Works without radius requirement
- Auto-populates cache on first run if needed

## Station Name Search

Search for stations by name, brand, or postcode within a specific radius. **Location is required** to prevent returning thousands of results.

Supports **regex patterns** for powerful searches:

```bash
# Simple text search (case-insensitive)
python3 uk_fuel_finder.py --station-name "tesco" --lat 51.5074 --lon -0.1278 --radius-miles 10

# Search by brand
python3 uk_fuel_finder.py --station-name "shell" --lat 53.8008 --lon -1.5491 --radius-miles 5

# Regex: starts with "BP " (note the space to exclude "BP M&S")
python3 uk_fuel_finder.py --station-name "^BP " --lat 51.5074 --lon -0.1278 --radius-km 8

# Regex: OR pattern - find Sainsbury's OR Tesco
python3 uk_fuel_finder.py --station-name "sainsbury|tesco" --lat 55.9533 --lon -3.1883 --radius-miles 10

# Search by postcode
python3 uk_fuel_finder.py --station-name "SW1A" --lat 51.5074 --lon -0.1278 --radius-miles 5

# Limit results
python3 uk_fuel_finder.py --station-name "jet" --lat 53.8008 --lon -1.5491 --radius-miles 15 --max-stations 5

# With specific fuel types
python3 uk_fuel_finder.py --station-name "esso" --lat 51.5074 --lon -0.1278 --radius-km 10 --fuel-types E5,B7_PREMIUM

# Example output:
{
  "state": 8,
  "mode": "station_name",
  "found": 8,
  "errors": 0,
  "details": {
    "icon": "mdi:gas-station",
    "best_e10": {
      "name": "TESCO EXTRA",
      "postcode": "SW1A 1AA",
      "miles": 0.5,
      "price": 139.9
    },
    "search_pattern": "tesco",
    "radius_miles": 10.0,
    "cache_updated": "2026-02-05T10:00:00.000Z",
    "newest_price": "2026-02-05T09:45:00.000Z"
  },
  "stations": [
    {
      "id": "abc123...",
      "name": "TESCO EXTRA",
      "brand": "TESCO",
      "postcode": "SW1A 1AA",
      "lat": 51.5074,
      "lon": -0.1278,
      "distance_miles": 0.5,
      "distance_km": 0.8,
      "open_today": "06:00-23:00",
      "is_motorway": false,
      "is_supermarket": true,
      "e10_price": 139.9,
      "e10_updated": "2026-02-05T10:30:00Z",
      "b7_standard_price": 147.9,
      "b7_standard_updated": "2026-02-05T10:30:00Z"
    },
    ...
  ],
  "last_update": "2026-02-05T10:30:00"
}
```

### Regex Pattern Examples

| Pattern | Matches | Example |
|---------|---------|---------|
| `tesco` | Contains "tesco" anywhere | "TESCO EXTRA", "Tesco Express" |
| `^BP ` | Starts with "BP " | "BP Station" (not "BP M&S") |
| `shell\|esso` | Contains "shell" OR "esso" | "Shell", "Esso", "Shell Express" |
| `^jet$` | Exactly "jet" | "JET" (not "JET STOP") |
| `SW1A\|W1` | Postcode starts with SW1A or W1 | "SW1A 1AA", "W1T 1AA" |
| `(?!.*motorway)shell` | "shell" but not "motorway" | Exclude motorway stations |

**Features:**
- Case-insensitive regex matching
- Searches name, brand, and postcode fields
- **Location required** (filters to radius FIRST, then searches)
- Sorted by distance (nearest first)
- Use `--max-stations` to limit results
- Auto-populates cache on first run if needed

**Why location is required:** Without filtering by radius first, searching for "shell" would return 500+ stations nationwide, which is impractical and slow.

## Configuration File

Default location: `/config/scripts/uk_fuel_finder_config.json`

```json
{
  "client_id": "your_client_id",
  "client_secret": "your_client_secret",
  "token_file": "/config/.storage/uk_fuel_finder/token.json",
  "state_file": "/config/.storage/uk_fuel_finder/state.json",
  "stations_baseline_days": 30,
  "stations_incremental_hours": 1,
  "prices_incremental_hours": 1
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `client_id` | string | Required | API client ID from Fuel Finder portal |
| `client_secret` | string | Required | API client secret |
| `token_file` | string | See above | OAuth token cache location |
| `state_file` | string | See above | Stations/prices cache location |
| `stations_baseline_days` | number | 30 | Full redownload interval (days) |
| `stations_incremental_hours` | number | 1 | Station update check interval (hours) |
| `prices_incremental_hours` | number | 1 | Price update check interval (hours) |

## Troubleshooting

### Cache seems stale

```bash
# Force full refresh
python3 uk_fuel_finder.py --lat 51.5074 --lon -0.1278 --radius-miles 10 --full-refresh --debug
```

### First run is slow

**Normal!** First run downloads all UK stations (~7,000 records, 3 minutes). Subsequent runs use cache (<1 second).

### No stations found

- Verify coordinates are correct (use [LatLong.net](https://www.latlong.net/))
- Increase search radius (`--radius-miles 20`)
- Check if stations exist in your area on Google Maps

### API timeouts

The script automatically retries with exponential backoff. If persistent:
- Check API status at https://www.fuel-finder.service.gov.uk/
- Try again in a few minutes
- Use `--debug` to see detailed progress

### Multiple sensors interfering

File locking handles this automatically. If you still see issues, check for stale lock files:

```bash
rm /config/.storage/uk_fuel_finder/state.json.lock
```

### Authentication errors

- Verify `client_id` and `client_secret` are correct
- Ensure you're using production credentials (not test)
- Delete token file to force fresh authentication:
  ```bash
  rm /config/.storage/uk_fuel_finder/token.json
  ```

## API Rate Limits

- **Live environment:** 120 requests/minute, 10,000 requests/day
- **Script efficiency:** 
  - Baseline: ~17 requests (once per month)
  - Incremental: ~1-3 requests (once per hour)
  - Well under limits even with 10+ sensors

## File Structure

```
/config/
├── scripts/
│   ├── uk_fuel_finder.py              # Main script
│   └── uk_fuel_finder_config.json     # Configuration
└── .storage/
    └── uk_fuel_finder/
        ├── token.json              # OAuth token cache
        ├── state.json              # Stations & prices cache (~3-5MB)
        └── state.json.lock         # Lock file (auto-cleanup)
```

## Requirements

- Python 3.7+
- UK Fuel Finder API credentials ([free registration](https://www.fuel-finder.service.gov.uk/))
- Unix-like OS (uses `fcntl` for file locking)

## Credits

- Built for Home Assistant community: https://community.home-assistant.io/t/uk-fuel-finder/982348/6
- Uses UK Government Fuel Finder API
- API documentation: https://www.fuel-finder.service.gov.uk/

## Changelog

### v2.0.0 (2026-02-06)
- **Unified output structure** across all query modes (location, station_id, station_name)
- **Consistent station data** with all fields present (id, name, brand, postcode, lat/lon, distances, flags, fuel prices)
- **Three timestamps:** last_update (script run), cache_updated (API fetch), newest_price (latest price change)
- **Smart sorting:** --sort-by-price flag > location provided > alphabetical
- **Dynamic best_<fuel> blocks** only appear when fuel type found in results
- Refactored to shared cache architecture
- Added incremental updates (stations + prices)
- Added multi-sensor support with file locking
- Added `--fuel-types` for flexible fuel tracking (E10, E5, B7_STANDARD, B7_PREMIUM, HVO, B10)
- Added `--max-stations` to limit results
- Added `--sort-by-price` option
- Added `--healthcheck` mode for cache validation
- Added `--station-id` for specific station lookups
- Added `--station-name` for regex-based searching by name/brand/postcode (requires location)
- Auto-populates cache on first run for all modes
- Improved file locking with automatic cleanup
- Fixed timezone handling in healthcheck mode
- Fixed multi-fuel type bug (all fuel types now populate correctly)
- Enhanced documentation with example coordinates (no personal data)

### v1.0.0 (2026-02-03)
- Initial release
- Basic station caching
- E10 and B7 tracking only
- Single location support
