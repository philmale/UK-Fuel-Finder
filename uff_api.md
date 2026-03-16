# UK Fuel Finder API Specification v1.3

Last Updated: 16 Mar 2026

This is the API specification that uff.py follows. It is not an official document - for that refer to the UK Government official web pages. However, this is my view of the specification as it stands as of the last updated date.

**Base URL:** `https://www.fuel-finder.service.gov.uk`

**Registration:** [https://www.fuel-finder.service.gov.uk/](https://www.fuel-finder.service.gov.uk/)

---

## 1. Authentication

The API uses OAuth2 Client Credentials flow. Generate an access token using your `client_id` and `client_secret`, then include it in subsequent requests as `Authorization: Bearer <token>`.

Tokens expire after 3,600 seconds (1 hour). Use the refresh token endpoint to obtain a new access token without re-authenticating.

---

### 1.1 Generate Access Token

Generates an OAuth access token using client credentials.

```
POST /api/v1/oauth/generate_access_token
```

**Content-Type:** `application/json`

#### Request Body

| Field           | Type   | Required | Description                     |
|-----------------|--------|----------|---------------------------------|
| `client_id`     | string | Yes      | Your registered client ID       |
| `client_secret` | string | Yes      | Your registered client secret   |

#### Request Example

```json
{
  "client_id": "laHVL2vCVCZ5wSOyDJ9ecNLcabt6FGl5",
  "client_secret": "nUYW4TDOme7aUKNmRceHZ8ffMevisFwlkKT9c89bGqhTJtgC4voUTn3QZPvxyQXJ"
}
```

#### Responses

**`200` — Access token generated successfully**

```json
{
  "success": true,
  "data": {
    "access_token": "632ab214482946527e7d7e5f522d4019639add5ebd20795b0d5fd8d19b565153",
    "token_type": "Bearer",
    "expires_in": 3600,
    "refresh_token": "7ad38ea6dbcf1123aef61785b0d6a8f3455bb68734080e0befa440c6ca6ee0eb"
  },
  "message": "Operation successful"
}
```

| Field                | Type    | Description                              |
|----------------------|---------|------------------------------------------|
| `success`            | boolean | Whether the request succeeded            |
| `data.access_token`  | string  | Bearer token for authenticating requests |
| `data.token_type`    | string  | Always `"Bearer"`                        |
| `data.expires_in`    | integer | Token lifetime in seconds (default 3600) |
| `data.refresh_token` | string  | Token for refreshing without re-auth     |
| `message`            | string  | Human-readable status message            |

**`400` — Invalid request payload**

```json
{
  "success": false,
  "message": "client_id or client_secret missing or invalid"
}
```

**`401` — Unauthorized (invalid client credentials)**

```json
{
  "success": false,
  "message": "Invalid client_id or client_secret"
}
```

**`500` — Internal server error**

```json
{
  "success": false,
  "statusCode": 500,
  "error": "Something went wrong"
}
```

---

### 1.2 Regenerate Access Token (Refresh)

Regenerates an access token using a previously issued refresh token, without requiring the client secret.

```
POST /api/v1/oauth/regenerate_access_token
```

**Content-Type:** `application/json`

#### Request Body

| Field           | Type   | Required | Description                                   |
|-----------------|--------|----------|-----------------------------------------------|
| `client_id`     | string | Yes      | Your registered client ID                     |
| `refresh_token` | string | Yes      | Refresh token from a prior token generation   |

#### Responses

**`200` — Access token regenerated successfully**

Response schema is identical to [1.1 Generate Access Token](#11-generate-access-token) `200` response.

**`400` — Invalid refresh token or client ID**

**`401` — Unauthorized (refresh token expired or revoked)**

**`500` — Internal server error**

---

## 2. Rate Limiting

Rate limits are applied separately to Information Recipient APIs and Price Submission APIs. As uff.py is a read-only consumer it is only subject to the Information Recipient limits.

### 2.1 Information Recipient APIs

| Limit | Value |
|-------|-------|
| Requests per minute per client | 30 |

A request must not be sent until the response to the previous request has been received. If a new request is sent before the previous one completes, the API returns **HTTP 429**.

HTTP 429 is also returned for general rate limit breaches. In both cases, back off and retry — do not send a new request until the previous one has completed or the retry window has elapsed.

uff.py's default `batch_sleep_seconds` of 4.0 seconds between batch pages gives approximately 15 requests per minute, comfortably within the 30 rpm limit. The `request_with_retry` function handles 429 responses with exponential backoff automatically.

### 2.2 Price Submission APIs

Not applicable to uff.py (read-only consumer). Documented here for completeness.

| Limit | Value |
|-------|-------|
| Requests per 5-minute window | 10 |
| Response on breach | HTTP 403 |
| Wait required after breach | 5 minutes |

---

## 3. Information Recipient APIs

APIs to fetch fuel prices and PFS (Petrol Fuel Station) information. All endpoints require OAuth2 Bearer token authentication.

**Pagination:** Each API response returns data for up to **500** records per batch. Increment the `batch-number` query parameter to retrieve subsequent pages. A response containing fewer than 500 records indicates the final batch.

**Common authorisation header:**

```
Authorization: Bearer <access_token>
```

---

### 3.1 Fetch All PFS Fuel Prices

Returns current fuel prices for all stations (full baseline).

```
GET /api/v1/pfs/fuel-prices?batch-number={n}
```

#### Query Parameters

| Parameter      | Type    | Required | Description                          |
|----------------|---------|----------|--------------------------------------|
| `batch-number` | integer | Yes      | Page number for paginated results (starts at 1) |

#### Response — `200` Fuel prices fetched successfully

Returns a JSON array of station price objects:

```json
[
  {
    "node_id": "0028acef5f3afc41c7e7d56fb285a940dfb64d6fea01cb4accd79c148321112d",
    "public_phone_number": null,
    "trading_name": "FORECOURT 4",
    "fuel_prices": [
      {
        "price": null,
        "fuel_type": "B10",
        "price_last_updated": null
      },
      {
        "price": "0120.0000",
        "fuel_type": "E10",
        "price_last_updated": "2025-12-31T08:15:23"
      },
      {
        "price": "0235.9000",
        "fuel_type": "B7_STANDARD",
        "price_last_updated": "2025-12-31T13:16:29"
      }
    ]
  }
]
```

#### Fuel Price Object Schema

| Field                      | Type         | Description                                               |
|----------------------------|--------------|-----------------------------------------------------------|
| `node_id`                  | string       | Unique station identifier (SHA-256 hash)                  |
| `public_phone_number`      | string\|null | Public contact number                                     |
| `trading_name`             | string       | Station trading name                                      |
| `fuel_prices`              | array        | Array of fuel price entries for this station              |
| `fuel_prices[].price`      | string\|null | Price in pence as a decimal string (e.g. `"0120.0000"` = 120.0p), or `null` if unavailable |
| `fuel_prices[].fuel_type`  | string       | Fuel type identifier (see [Fuel Types](#fuel-types))      |
| `fuel_prices[].price_last_updated` | string\|null | ISO 8601 datetime of last price update (no timezone; treat as UTC), or `null` |

> **Note:** `mft_organisation_name` was removed from all API responses in the 25–26 Feb 2026 release. It is no longer present in any endpoint response.

#### Error Responses

**`401` — Unauthorized**

```json
{
  "error": "Unauthorized",
  "message": "Invalid API key or missing authentication header."
}
```

**`429` — Rate limit exceeded or previous request not yet complete**

Retry after backing off. Do not send a new request until the previous one has received a response.

**`500` — Internal server error**

```json
{
  "success": false,
  "statusCode": 500,
  "error": "Something went wrong"
}
```

---

### 3.2 Fetch Incremental PFS Fuel Prices

Returns fuel prices updated since the given timestamp. Same response schema as [3.1](#31-fetch-all-pfs-fuel-prices), but filtered to only include stations with price changes after the specified time.

```
GET /api/v1/pfs/fuel-prices?batch-number={n}&effective-start-timestamp={timestamp}
```

#### Query Parameters

| Parameter                    | Type    | Required | Description                                              |
|------------------------------|---------|----------|----------------------------------------------------------|
| `batch-number`               | integer | Yes      | Page number for paginated results (starts at 1)          |
| `effective-start-timestamp`  | string  | Yes      | Return prices updated after this time. Format: `YYYY-MM-DD HH:MM:SS` |

#### Example Request

```
GET /api/v1/pfs/fuel-prices?batch-number=1&effective-start-timestamp=2026-01-12 00:00:00
```

#### Response — `200`

Same schema as [3.1](#31-fetch-all-pfs-fuel-prices). Only stations with price updates after the specified timestamp are returned. Stations may include only the changed fuel types in their `fuel_prices` array.

#### Error Responses

Same as [3.1](#31-fetch-all-pfs-fuel-prices).

---

### 3.3 Fetch All PFS Station Information

Returns full station details for all registered petrol fuel stations (full baseline).

```
GET /api/v1/pfs?batch-number={n}
```

#### Query Parameters

| Parameter      | Type    | Required | Description                          |
|----------------|---------|----------|--------------------------------------|
| `batch-number` | integer | Yes      | Page number for paginated results (starts at 1) |

#### Response — `200` PFS info fetched successfully

Returns a JSON array of station objects:

```json
[
  {
    "node_id": "9b275ab576eeba3c6677984be15ee22a74e54fdfe8e5ea700e84a03178dc4ac1",
    "public_phone_number": null,
    "trading_name": "TEST",
    "is_same_trading_and_brand_name": true,
    "brand_name": "TEST",
    "temporary_closure": false,
    "permanent_closure": false,
    "permanent_closure_date": null,
    "is_motorway_service_station": false,
    "is_supermarket_service_station": false,
    "location": {
      "address_line_1": "HALL & WOODHOUSE, TAPLOW BOATYARD, MILL LANE, TAPLOW, MAIDENHEAD, SL6 0AA",
      "address_line_2": null,
      "city": "MAIDENHEAD",
      "country": "England",
      "county": null,
      "postcode": "SL6 0AA",
      "latitude": "51.5268585",
      "longitude": "-0.7003610"
    },
    "amenities": [
      "water_filling"
    ],
    "opening_times": {
      "usual_days": {
        "monday": {
          "open": "00:00:00",
          "close": "00:00:00",
          "is_24_hours": false
        }
      },
      "bank_holiday": {
        "type": "bank holiday",
        "open_time": "00:00:00",
        "close_time": "00:00:00",
        "is_24_hours": false
      }
    },
    "fuel_types": [
      "E10",
      "E5",
      "HVO",
      "B10"
    ]
  }
]
```

#### Station Object Schema

| Field                              | Type         | Description                                     |
|------------------------------------|--------------|-------------------------------------------------|
| `node_id`                          | string       | Unique station identifier (SHA-256 hash)        |
| `public_phone_number`              | string\|null | Public contact number                           |
| `trading_name`                     | string       | Station trading name                            |
| `is_same_trading_and_brand_name`   | boolean      | Whether trading and brand names match           |
| `brand_name`                       | string       | Station brand name                              |
| `temporary_closure`                | boolean      | Whether the station is temporarily closed       |
| `permanent_closure`                | boolean\|null | Whether the station is permanently closed      |
| `permanent_closure_date`           | string\|null | ISO date of permanent closure, if applicable    |
| `is_motorway_service_station`      | boolean      | Whether located at a motorway service area      |
| `is_supermarket_service_station`   | boolean      | Whether attached to a supermarket               |
| `location`                         | object       | Station location details (see below)            |
| `amenities`                        | array        | List of amenity identifiers (see [Amenities](#amenities)) |
| `opening_times`                    | object       | Opening hours (see below)                       |
| `fuel_types`                       | array        | Fuel types sold at this station (see [Fuel Types](#fuel-types)) |

> **Note:** `mft_organisation_name` was removed from all API responses in the 25–26 Feb 2026 release. It is no longer present in this or any other endpoint response.

#### Location Object

| Field           | Type         | Description                          |
|-----------------|--------------|--------------------------------------|
| `address_line_1`| string       | Primary address line                 |
| `address_line_2`| string\|null | Secondary address line               |
| `city`          | string       | City or town                         |
| `country`       | string       | Country (e.g. `"England"`)           |
| `county`        | string\|null | County                               |
| `postcode`      | string       | UK postcode                          |
| `latitude`      | string       | Latitude as decimal string           |
| `longitude`     | string       | Longitude as decimal string          |

#### Opening Times Object

`usual_days` contains an entry for each day of the week (`monday` through `sunday`):

| Field        | Type    | Description                              |
|--------------|---------|------------------------------------------|
| `open`       | string  | Opening time in `HH:MM:SS` format        |
| `close`      | string  | Closing time in `HH:MM:SS` format        |
| `is_24_hours`| boolean | Whether the station is open 24 hours     |

`bank_holiday` contains:

| Field        | Type    | Description                              |
|--------------|---------|------------------------------------------|
| `type`       | string  | Holiday type (e.g. `"bank holiday"`, `"standard"`) |
| `open_time`  | string  | Opening time in `HH:MM:SS` format        |
| `close_time` | string  | Closing time in `HH:MM:SS` format        |
| `is_24_hours`| boolean | Whether open 24 hours on bank holidays   |

#### Error Responses

Same as [3.1](#31-fetch-all-pfs-fuel-prices).

---

### 3.4 Fetch Incremental PFS Station Information

Returns station information updated since the given timestamp. Same response schema as [3.3](#33-fetch-all-pfs-station-information).

```
GET /api/v1/pfs?batch-number={n}&effective-start-timestamp={timestamp}
```

#### Query Parameters

| Parameter                    | Type    | Required | Description                                              |
|------------------------------|---------|----------|----------------------------------------------------------|
| `batch-number`               | integer | Yes      | Page number for paginated results (starts at 1)          |
| `effective-start-timestamp`  | string  | Yes      | Return stations updated after this time. Format: `YYYY-MM-DD HH:MM:SS` |

#### Example Request

```
GET /api/v1/pfs?batch-number=1&effective-start-timestamp=2026-01-10 00:00:00
```

#### Response — `200`

Same schema as [3.3](#33-fetch-all-pfs-station-information).

#### Error Responses

Same as [3.1](#31-fetch-all-pfs-fuel-prices).

---

## Appendix

### Fuel Types

| Identifier     | Description                      |
|----------------|----------------------------------|
| `E10`          | Petrol (up to 10% ethanol)       |
| `E5`           | Premium petrol (up to 5% ethanol)|
| `B7_STANDARD`  | Standard diesel (up to 7% bio)   |
| `B7_PREMIUM`   | Premium diesel (up to 7% bio)    |
| `B10`          | Diesel (up to 10% bio)           |
| `HVO`          | Hydrotreated Vegetable Oil       |

### Amenities

Known amenity identifiers observed in API responses:

`adblue_packaged`, `adblue_pumps`, `car_wash`, `customer_toilets`, `water_filling`

### Price Format Notes

Prices are returned as decimal strings in pence (e.g. `"0120.0000"` = 120.0 pence per litre). Some stations report prices in pounds rather than pence (values below `2.0`); consumers should detect and correct these by multiplying by 100. Prices may be `null` for fuel types the station has registered but not yet priced.

The developer portal warns submitters if a submitted price is below 100 ppl or above 999 ppl, which should over time reduce the frequency of pound-vs-pence entry errors in the live data.

### Timestamp Format

All timestamps in price data use the format `YYYY-MM-DDTHH:MM:SS` with no timezone indicator. Treat as UTC.

The `effective-start-timestamp` query parameter uses the format `YYYY-MM-DD HH:MM:SS` (space-separated, no `T`).

### Removed Fields

| Field                 | Removed      | Affected endpoints       |
|-----------------------|--------------|--------------------------|
| `mft_organisation_name` | 25 Feb 2026 | All API responses        |
| `mft.name`            | 25 Feb 2026  | CSV extracts only        |

### Endpoint Summary

| Method | Path                       | Description                              |
|--------|----------------------------|------------------------------------------|
| POST   | `/api/v1/oauth/generate_access_token`     | Generate new access + refresh token      |
| POST   | `/api/v1/oauth/regenerate_access_token`   | Refresh access token                     |
| GET    | `/api/v1/pfs/fuel-prices`                 | Fetch all fuel prices (baseline)         |
| GET    | `/api/v1/pfs/fuel-prices?effective-start-timestamp=...` | Fetch incremental fuel prices |
| GET    | `/api/v1/pfs`                             | Fetch all station information (baseline) |
| GET    | `/api/v1/pfs?effective-start-timestamp=...` | Fetch incremental station information  |

### Changelog

| Version | Date        | Changes                                                                 |
|---------|-------------|-------------------------------------------------------------------------|
| 1.3     | 16 Mar 2026 | Added rate limiting section (Section 2); added 429 error responses; noted `mft_organisation_name` removal; section numbering updated; price format note updated re portal validation warning |
| 1.2     | 26 Feb 2026 | Noted `mft_organisation_name` removal from API responses (25 Feb 2026 release) |
| 1.1     | —           | Initial public specification                                            |