#!/usr/bin/env python3
"""
Test script to verify REST Countries v5 API connectivity using the account key
configured in the project's local .env file.
"""

import json
import sys
import urllib.error
import urllib.parse
import urllib.request

# Reuse configuration loader from etl.py
from etl import get_api_key, load_environment

BASE_URL = "https://api.restcountries.com/countries/v5"

FIELDS = [
    "names.common",
    "names.official",
    "codes.alpha_2",
    "codes.alpha_3",
    "capitals",
    "region",
    "subregion",
    "population",
    "area.kilometers",
    "languages",
    "currencies",
    "coordinates",
    "continents",
]


def build_request_url(limit=10):
    query_params = {
        "limit": str(limit),
        "response_fields": ",".join(FIELDS),
    }
    return f"{BASE_URL}?{urllib.parse.urlencode(query_params)}"


def test_v5_api():
    # Load environment variables from local .env file
    load_environment()

    try:
        api_key = get_api_key()
    except ValueError as val_err:
        print(f"Configuration Error: {val_err}", file=sys.stderr)
        sys.exit(1)

    is_account_key = (api_key != "rc_live_demo")
    target_url = build_request_url(limit=10)

    print("Connecting to REST Countries v5 API...")
    print(f"Target URL: {target_url}")
    print(f"Authentication Mode: {'Account API Key (from .env)' if is_account_key else 'Public Demo Key'}")
    print("-" * 60)

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "User-Agent": "CountryAnalytics/1.0 (Testing Script)",
    }

    req = urllib.request.Request(target_url, headers=headers, method="GET")

    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            status_code = response.getcode()
            raw_bytes = response.read()
    except urllib.error.HTTPError as http_err:
        status_code = http_err.code
        print(f"HTTP Error: Status {status_code} - {http_err.reason}", file=sys.stderr)
        try:
            error_payload = json.loads(http_err.read().decode("utf-8"))
            print(f"Error Details: {error_payload}", file=sys.stderr)
        except Exception:
            pass
        sys.exit(1)
    except urllib.error.URLError as url_err:
        print(f"Network / URL Error: {url_err.reason}", file=sys.stderr)
        sys.exit(1)
    except TimeoutError:
        print("Request timed out after 30 seconds.", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"Unexpected error during network request: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"HTTP Status: {status_code} OK")

    # Parse JSON
    try:
        response_json = json.loads(raw_bytes.decode("utf-8"))
    except json.JSONDecodeError as json_err:
        print(f"Error: Response is not valid JSON ({json_err})", file=sys.stderr)
        sys.exit(1)

    if not isinstance(response_json, dict):
        print(f"Unexpected response structure: expected JSON object, got {type(response_json).__name__}", file=sys.stderr)
        sys.exit(1)

    top_level_keys = list(response_json.keys())
    print(f"Available top-level response keys: {top_level_keys}")

    # Check for API error response
    if response_json.get("success") is False or "errors" in response_json:
        errors = response_json.get("errors", [])
        print("\nAPI Error Message:")
        for err in errors:
            msg = err.get("message") if isinstance(err, dict) else str(err)
            print(f"  - {msg}")
        sys.exit(1)

    # Locate country objects array
    data_block = response_json.get("data", {})
    records = data_block.get("objects", []) if isinstance(data_block, dict) else []

    num_records = len(records)
    print(f"Number of country records returned: {num_records}")

    meta = data_block.get("meta", {}) if isinstance(data_block, dict) else {}
    if meta:
        print(f"Pagination Info: total={meta.get('total')}, count={meta.get('count')}, more={meta.get('more')}")

    if num_records == 0:
        print("Warning: Records array is empty.", file=sys.stderr)
        return

    # Print a few country names
    sample_names = [
        r.get("names", {}).get("common") or r.get("name", {}).get("common", "Unknown")
        for r in records[:5]
    ]
    print(f"Sample Country Names ({len(sample_names)} shown): {sample_names}")

    # Print first country details
    sample = records[0]
    print("\n--- First Country Record Summary ---")

    def get_nested(d, dot_key, default="N/A"):
        parts = dot_key.split(".")
        current = d
        for p in parts:
            if isinstance(current, dict) and p in current:
                current = current[p]
            else:
                return default
        return current if current is not None else default

    common_name = get_nested(sample, "names.common")
    official_name = get_nested(sample, "names.official")
    alpha_2 = get_nested(sample, "codes.alpha_2")
    alpha_3 = get_nested(sample, "codes.alpha_3")
    capitals = sample.get("capitals", "N/A")
    region = sample.get("region", "N/A")
    subregion = sample.get("subregion", "N/A")
    population = sample.get("population", "N/A")
    area_km = get_nested(sample, "area.kilometers")

    print(f"Common Name: {common_name}")
    print(f"Official Name: {official_name}")
    print(f"Alpha-2 / Alpha-3: {alpha_2} / {alpha_3}")
    print(f"Capital: {capitals}")
    print(f"Region / Subregion: {region} / {subregion}")
    print(f"Population: {population:,}" if isinstance(population, (int, float)) else f"Population: {population}")
    print(f"Area: {area_km:,.1f} sq km" if isinstance(area_km, (int, float)) else f"Area: {area_km}")


if __name__ == "__main__":
    test_v5_api()
