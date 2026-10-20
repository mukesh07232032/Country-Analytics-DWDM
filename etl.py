"""
Country Analytics - ETL Pipeline Module
Extracts country data from REST Countries v5 API, transforms records into
the analytical relational schema, and loads data into PostgreSQL idempotently.
"""

import datetime
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

# Set up module logger
logger = logging.getLogger("country_analytics.etl")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

API_BASE_URL = "https://api.restcountries.com/countries/v5"

DEFAULT_RESPONSE_FIELDS = [
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


# ==============================================================================
# PART 1: CONFIGURATION & ENVIRONMENT
# ==============================================================================

def load_environment(dotenv_path: Optional[str] = None) -> None:
    """
    Loads environment variables from .env if present.
    Uses python-dotenv if available, with a lightweight manual parser fallback.
    """
    try:
        import dotenv
        if dotenv_path:
            dotenv.load_dotenv(dotenv_path=dotenv_path, override=False)
        else:
            dotenv.load_dotenv(override=False)
        return
    except ImportError:
        pass

    # Lightweight fallback parser for .env
    target_path = dotenv_path or os.path.join(os.getcwd(), ".env")
    if os.path.isfile(target_path):
        try:
            with open(target_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip().strip("'\"")
                    if key not in os.environ:
                        os.environ[key] = val
        except Exception as e:
            logger.warning(f"Could not parse .env file: {e}")


def mask_secret(secret: Optional[str]) -> str:
    """Masks sensitive strings for safe logging."""
    if not secret:
        return "<EMPTY>"
    if len(secret) <= 8:
        return "***"
    return f"{secret[:4]}...{secret[-4:]}"


def get_api_key() -> str:
    """
    Retrieves the REST Countries API key from environment variables.
    Raises ValueError if missing.
    """
    load_environment()
    api_key = os.getenv("REST_COUNTRIES_API_KEY", "").strip()
    if not api_key:
        raise ValueError(
            "REST_COUNTRIES_API_KEY is not set. "
            "Please configure it in your environment or .env file."
        )
    return api_key


def get_db_config() -> Dict[str, Any]:
    """
    Retrieves PostgreSQL connection parameters from environment variables.
    """
    load_environment()
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "country_analytics"),
        "user": os.getenv("POSTGRES_USER", "airflow"),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
    }


# ==============================================================================
# PART 2: EXTRACTION
# ==============================================================================

def fetch_page(
    api_key: str,
    limit: int = 100,
    offset: int = 0,
    fields: Optional[List[str]] = None,
    timeout: int = 30,
    max_retries: int = 3,
    backoff_factor: float = 1.5,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], bool]:
    """
    Fetches a single page of country records from REST Countries v5.

    Returns:
        tuple: (objects_list, meta_dict, is_demo_flag)
    """
    selected_fields = fields or DEFAULT_RESPONSE_FIELDS
    query_params = {
        "limit": str(limit),
        "offset": str(offset),
        "response_fields": ",".join(selected_fields),
    }
    url = f"{API_BASE_URL}?{urllib.parse.urlencode(query_params)}"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "User-Agent": "CountryAnalytics/1.0 (ETL Pipeline)",
    }

    last_exception = None

    for attempt in range(max_retries + 1):
        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw_bytes = resp.read()
                break
        except urllib.error.HTTPError as http_err:
            last_exception = http_err
            status = http_err.code
            # Fatal authentication/authorization errors - do not retry indefinitely
            if status in (401, 403):
                raise PermissionError(
                    f"Authentication failed (HTTP {status}). Verify REST_COUNTRIES_API_KEY."
                ) from http_err

            # Transient errors eligible for retry
            if status in (429, 500, 502, 503, 504) and attempt < max_retries:
                sleep_seconds = backoff_factor * (attempt + 1)
                logger.warning(
                    f"Transient HTTP {status} error. Retrying in {sleep_seconds:.1f}s (attempt {attempt + 1}/{max_retries})..."
                )
                time.sleep(sleep_seconds)
                continue

            raise RuntimeError(
                f"REST Countries API request failed with HTTP {status}: {http_err.reason}"
            ) from http_err

        except (urllib.error.URLError, TimeoutError) as network_err:
            last_exception = network_err
            if attempt < max_retries:
                sleep_seconds = backoff_factor * (attempt + 1)
                logger.warning(
                    f"Network error ({network_err}). Retrying in {sleep_seconds:.1f}s (attempt {attempt + 1}/{max_retries})..."
                )
                time.sleep(sleep_seconds)
                continue
            raise RuntimeError(
                f"Network connection failed after {max_retries} retries: {network_err}"
            ) from network_err

    # Parse and validate JSON
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except json.JSONDecodeError as decode_err:
        raise ValueError(f"Failed to parse API response as JSON: {decode_err}") from decode_err

    if not isinstance(payload, dict):
        raise ValueError(
            f"Unexpected API response type: expected JSON object, got {type(payload).__name__}"
        )

    if payload.get("success") is False or "errors" in payload:
        errors = payload.get("errors", [])
        err_msg = "; ".join(e.get("message", str(e)) if isinstance(e, dict) else str(e) for e in errors)
        raise RuntimeError(f"REST Countries API returned error: {err_msg}")

    data_block = payload.get("data")
    if not isinstance(data_block, dict):
        raise ValueError(
            f"Unexpected payload format: missing 'data' object in response. Keys: {list(payload.keys())}"
        )

    objects = data_block.get("objects", [])
    if not isinstance(objects, list):
        raise ValueError(
            f"Unexpected 'objects' type in 'data': expected list, got {type(objects).__name__}"
        )

    meta = data_block.get("meta", {})
    if not isinstance(meta, dict):
        meta = {}

    is_demo = bool(data_block.get("_demo"))

    return objects, meta, is_demo


def extract_countries(
    api_key: Optional[str] = None,
    page_size: int = 100,
    timeout: int = 30,
    max_pages: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Extracts all country records from REST Countries v5 across pages.

    Args:
        api_key: REST Countries API key (retrieved from environment if omitted).
        page_size: Records requested per page (maximum 100 on free plan).
        timeout: Request timeout in seconds.
        max_pages: Optional limit on the number of pages to fetch (useful for testing).

    Returns:
        List of raw country dictionaries.
    """
    key = api_key or get_api_key()
    logger.info(f"Beginning country extraction with API key: {mask_secret(key)}")

    all_records: List[Dict[str, Any]] = []
    offset = 0
    page_count = 0

    # Ensure page size does not exceed free plan maximum
    bounded_limit = max(1, min(page_size, 100))

    while True:
        page_count += 1
        logger.info(f"Fetching page {page_count} (limit={bounded_limit}, offset={offset})...")

        objects, meta, is_demo = fetch_page(
            api_key=key,
            limit=bounded_limit,
            offset=offset,
            timeout=timeout,
        )

        num_fetched = len(objects)
        logger.info(f"Page {page_count} returned {num_fetched} records.")

        if num_fetched > 0:
            all_records.extend(objects)

        # Stop conditions based on verified v5 API behavior
        if is_demo:
            logger.info("Demo key response detected: returning single sample country.")
            break

        if num_fetched == 0:
            logger.info("Empty page returned; end of dataset reached.")
            break

        if meta.get("more") is False:
            logger.info("API metadata indicates no more records (more=False).")
            break

        total_available = meta.get("total")
        if total_available is not None and (offset + num_fetched) >= total_available:
            logger.info(f"Reached total available records ({offset + num_fetched} / {total_available}).")
            break

        if num_fetched < bounded_limit:
            logger.info(f"Page returned fewer than requested items ({num_fetched} < {bounded_limit}); terminating.")
            break

        if max_pages is not None and page_count >= max_pages:
            logger.info(f"Reached max_pages limit ({max_pages}).")
            break

        offset += num_fetched

    logger.info(f"Extraction complete: retrieved {len(all_records)} total records across {page_count} page(s).")
    return all_records


# ==============================================================================
# PART 3: TRANSFORMATION
# ==============================================================================

def transform_capital(capitals_data: Any) -> Optional[str]:
    """
    Extracts the primary capital name from v5 structured capital objects.
    Selects primary capital if identified, otherwise takes the first valid capital name.
    Returns None if no valid capital exists.
    """
    if not capitals_data:
        return None

    if isinstance(capitals_data, list):
        if not capitals_data:
            return None

        # Look for primary capital attribute
        for item in capitals_data:
            if isinstance(item, dict):
                attrs = item.get("attributes")
                if isinstance(attrs, dict) and attrs.get("primary") is True:
                    name = item.get("name")
                    if name and str(name).strip():
                        return str(name).strip()

        # Fallback to first dict with a valid name
        for item in capitals_data:
            if isinstance(item, dict):
                name = item.get("name")
                if name and str(name).strip():
                    return str(name).strip()
            elif isinstance(item, str) and item.strip():
                return item.strip()

    elif isinstance(capitals_data, str) and capitals_data.strip():
        return capitals_data.strip()

    return None


def transform_population_density(population: Any, area_km2: Any) -> Optional[float]:
    """
    Computes population density (population / area_km2).
    Returns None if population or area is missing, invalid, or area <= 0.
    """
    if population is None or area_km2 is None:
        return None

    try:
        pop = float(population)
        area = float(area_km2)
    except (ValueError, TypeError):
        return None

    if pop < 0 or area <= 0:
        return None

    return round(pop / area, 4)


def transform_country(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Transforms a raw API country object into the schema required for the 'countries' table.
    Returns None if mandatory primary key or name is missing/malformed.
    """
    if not isinstance(raw, dict):
        return None

    # Country code (ISO 3166-1 alpha-3) - Primary Key
    codes_block = raw.get("codes") if isinstance(raw.get("codes"), dict) else {}
    country_code = codes_block.get("alpha_3") or raw.get("cca3") or raw.get("country_code")
    if not country_code or not isinstance(country_code, str):
        return None
    country_code = country_code.strip().upper()
    if len(country_code) != 3:
        return None

    # Alpha-2 code
    alpha2_code = codes_block.get("alpha_2") or raw.get("cca2")
    if alpha2_code and isinstance(alpha2_code, str):
        alpha2_code = alpha2_code.strip().upper()[:2]
    else:
        alpha2_code = None

    # Names
    names_block = raw.get("names") if isinstance(raw.get("names"), dict) else {}
    name_fallback = raw.get("name") if isinstance(raw.get("name"), dict) else {}

    country_name = names_block.get("common") or name_fallback.get("common") or raw.get("name")
    if not country_name or not isinstance(country_name, str) or not country_name.strip():
        return None
    country_name = country_name.strip()[:150]

    official_name = names_block.get("official") or name_fallback.get("official")
    if official_name and isinstance(official_name, str):
        official_name = official_name.strip()
    else:
        official_name = None

    # Capital
    capitals_raw = raw.get("capitals") if "capitals" in raw else raw.get("capital")
    capital = transform_capital(capitals_raw)

    # Region & Subregion
    region = raw.get("region")
    region = str(region).strip()[:100] if region and isinstance(region, str) else None

    subregion = raw.get("subregion")
    subregion = str(subregion).strip()[:100] if subregion and isinstance(subregion, str) else None

    # Population
    population_raw = raw.get("population")
    population: Optional[int] = None
    if population_raw is not None:
        try:
            pop_int = int(population_raw)
            if pop_int >= 0:
                population = pop_int
        except (ValueError, TypeError):
            population = None

    # Area in square kilometers
    area_raw = raw.get("area")
    area_km2: Optional[float] = None
    if isinstance(area_raw, dict):
        area_val = area_raw.get("kilometers")
    else:
        area_val = area_raw

    if area_val is not None:
        try:
            area_float = float(area_val)
            if area_float >= 0:
                area_km2 = area_float
        except (ValueError, TypeError):
            area_km2 = None

    # Coordinates (Latitude & Longitude)
    coords_raw = raw.get("coordinates")
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    if isinstance(coords_raw, dict):
        lat_val = coords_raw.get("lat")
        lng_val = coords_raw.get("lng")
        if lat_val is not None:
            try:
                latitude = float(lat_val)
            except (ValueError, TypeError):
                pass
        if lng_val is not None:
            try:
                longitude = float(lng_val)
            except (ValueError, TypeError):
                pass
    elif isinstance(raw.get("latlng"), (list, tuple)) and len(raw["latlng"]) >= 2:
        try:
            latitude = float(raw["latlng"][0])
            longitude = float(raw["latlng"][1])
        except (ValueError, TypeError):
            pass

    # Computed Population Density
    population_density = transform_population_density(population, area_km2)

    return {
        "country_code": country_code,
        "alpha2_code": alpha2_code,
        "country_name": country_name,
        "official_name": official_name,
        "capital": capital,
        "region": region,
        "subregion": subregion,
        "population": population,
        "area_km2": area_km2,
        "latitude": latitude,
        "longitude": longitude,
        "population_density": population_density,
        "last_updated": datetime.datetime.now(datetime.timezone.utc),
    }


def transform_languages(country_code: str, raw_languages: Any) -> List[Dict[str, Any]]:
    """
    Extracts structured language records for a country.
    Returns deduplicated list of {country_code, language_code, language_name, native_name}.
    """
    if not country_code or not raw_languages:
        return []

    languages: List[Dict[str, Any]] = []
    seen_codes = set()

    # V5 list of language objects
    if isinstance(raw_languages, list):
        for item in raw_languages:
            if not isinstance(item, dict):
                continue
            # Prefer BCP47 tag or ISO-639 identifiers
            lang_code = (
                item.get("bcp47")
                or item.get("iso639_3")
                or item.get("iso639_1")
                or item.get("code")
            )
            lang_name = item.get("name")
            native_name = item.get("native_name")

            if not lang_code or not lang_name:
                continue

            clean_code = str(lang_code).strip()[:30]
            clean_name = str(lang_name).strip()
            clean_native = str(native_name).strip() if native_name else None

            if clean_code and clean_code not in seen_codes:
                seen_codes.add(clean_code)
                languages.append({
                    "country_code": country_code,
                    "language_code": clean_code,
                    "language_name": clean_name,
                    "native_name": clean_native,
                })

    # Legacy mapping {code: name}
    elif isinstance(raw_languages, dict):
        for code, name in raw_languages.items():
            if not code or not name:
                continue
            clean_code = str(code).strip()[:30]
            clean_name = str(name).strip()
            if clean_code and clean_code not in seen_codes:
                seen_codes.add(clean_code)
                languages.append({
                    "country_code": country_code,
                    "language_code": clean_code,
                    "language_name": clean_name,
                    "native_name": None,
                })

    return languages


def transform_currencies(country_code: str, raw_currencies: Any) -> List[Dict[str, Any]]:
    """
    Extracts structured currency records for a country.
    Returns deduplicated list of {country_code, currency_code, currency_name, symbol}.
    """
    if not country_code or not raw_currencies:
        return []

    currencies: List[Dict[str, Any]] = []
    seen_codes = set()

    # V5 list of currency objects
    if isinstance(raw_currencies, list):
        for item in raw_currencies:
            if not isinstance(item, dict):
                continue
            curr_code = item.get("code")
            if not curr_code:
                continue
            clean_code = str(curr_code).strip().upper()[:10]
            clean_name = str(item.get("name")).strip() if item.get("name") else None
            clean_symbol = str(item.get("symbol")).strip() if item.get("symbol") else None

            if clean_code and clean_code not in seen_codes:
                seen_codes.add(clean_code)
                currencies.append({
                    "country_code": country_code,
                    "currency_code": clean_code,
                    "currency_name": clean_name,
                    "symbol": clean_symbol,
                })

    # Legacy mapping {code: {name, symbol}}
    elif isinstance(raw_currencies, dict):
        for code, details in raw_currencies.items():
            if not code:
                continue
            clean_code = str(code).strip().upper()[:10]
            clean_name = None
            clean_symbol = None
            if isinstance(details, dict):
                clean_name = str(details.get("name")).strip() if details.get("name") else None
                clean_symbol = str(details.get("symbol")).strip() if details.get("symbol") else None
            elif isinstance(details, str):
                clean_name = details.strip()

            if clean_code and clean_code not in seen_codes:
                seen_codes.add(clean_code)
                currencies.append({
                    "country_code": country_code,
                    "currency_code": clean_code,
                    "currency_name": clean_name,
                    "symbol": clean_symbol,
                })

    return currencies


def transform_dataset(raw_countries: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Transforms a collection of raw country payloads into relational table batches.
    Returns:
        {
            "countries": [...],
            "languages": [...],
            "currencies": [...]
        }
    """
    countries: List[Dict[str, Any]] = []
    languages: List[Dict[str, Any]] = []
    currencies: List[Dict[str, Any]] = []
    rejected_records: List[Dict[str, Any]] = []

    seen_country_codes = set()

    for raw in raw_countries:
        country_record = transform_country(raw)
        if not country_record:
            name = (
                (raw.get("names") or {}).get("common")
                or (raw.get("name") or {}).get("common")
                or "Unknown"
            )
            codes = raw.get("codes") or {}
            a3 = codes.get("alpha_3") or raw.get("cca3") or raw.get("country_code")
            reason = (
                "Missing or invalid 3-letter alpha-3 country code"
                if not a3 or len(str(a3).strip()) != 3
                else "Missing or invalid common name"
            )
            logger.warning(f"Rejected country record '{name}': {reason} (codes={codes})")
            rejected_records.append({
                "country_name": name,
                "reason": reason,
                "codes": codes,
            })
            continue

        c_code = country_record["country_code"]
        if c_code in seen_country_codes:
            logger.warning(f"Duplicate country code '{c_code}' encountered for '{country_record['country_name']}'; skipping duplicate.")
            rejected_records.append({
                "country_name": country_record["country_name"],
                "reason": f"Duplicate country code {c_code}",
                "codes": {"alpha_3": c_code},
            })
            continue
        seen_country_codes.add(c_code)

        countries.append(country_record)

        lang_records = transform_languages(c_code, raw.get("languages"))
        languages.extend(lang_records)

        curr_records = transform_currencies(c_code, raw.get("currencies"))
        currencies.extend(curr_records)

    return {
        "countries": countries,
        "languages": languages,
        "currencies": currencies,
        "rejected_records": rejected_records,
    }


# ==============================================================================
# PART 4 & 5: DATABASE LOADING & IDEMPOTENCY
# ==============================================================================

def get_db_connection(db_config: Optional[Dict[str, Any]] = None):
    """
    Creates and returns a psycopg2 database connection.
    Lazy-loads psycopg2 to prevent import errors during non-database tasks.
    """
    try:
        import psycopg2
    except ImportError as e:
        raise ImportError(
            "psycopg2 is required for database operations. "
            "Install it via requirements.txt or run inside the container."
        ) from e

    config = db_config or get_db_config()
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"],
    )


def load_data(
    transformed_data: Dict[str, List[Dict[str, Any]]],
    db_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, int]:
    """
    Loads transformed country, language, and currency records into PostgreSQL idempotently.
    Uses parameterized SQL and transactions. Cleans up stale child rows for refreshed countries.

    Returns:
        Dictionary with count of records upserted.
    """
    countries = transformed_data.get("countries", [])
    languages = transformed_data.get("languages", [])
    currencies = transformed_data.get("currencies", [])

    if not countries:
        logger.info("No country records to load.")
        return {"countries_loaded": 0, "languages_loaded": 0, "currencies_loaded": 0}

    country_codes = [c["country_code"] for c in countries]

    conn = get_db_connection(db_config)

    upsert_country_sql = """
        INSERT INTO countries (
            country_code, alpha2_code, country_name, official_name,
            capital, region, subregion, population, area_km2,
            latitude, longitude, population_density, last_updated
        ) VALUES (
            %(country_code)s, %(alpha2_code)s, %(country_name)s, %(official_name)s,
            %(capital)s, %(region)s, %(subregion)s, %(population)s, %(area_km2)s,
            %(latitude)s, %(longitude)s, %(population_density)s, %(last_updated)s
        )
        ON CONFLICT (country_code) DO UPDATE SET
            alpha2_code = EXCLUDED.alpha2_code,
            country_name = EXCLUDED.country_name,
            official_name = EXCLUDED.official_name,
            capital = EXCLUDED.capital,
            region = EXCLUDED.region,
            subregion = EXCLUDED.subregion,
            population = EXCLUDED.population,
            area_km2 = EXCLUDED.area_km2,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            population_density = EXCLUDED.population_density,
            last_updated = EXCLUDED.last_updated;
    """

    insert_language_sql = """
        INSERT INTO country_languages (country_code, language_code, language_name, native_name)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (country_code, language_code) DO UPDATE SET
            language_name = EXCLUDED.language_name,
            native_name = EXCLUDED.native_name;
    """

    insert_currency_sql = """
        INSERT INTO country_currencies (country_code, currency_code, currency_name, symbol)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (country_code, currency_code) DO UPDATE SET
            currency_name = EXCLUDED.currency_name,
            symbol = EXCLUDED.symbol;
    """

    try:
        with conn:
            with conn.cursor() as cur:
                # 1. Upsert countries
                cur.executemany(upsert_country_sql, countries)
                logger.info(f"Upserted {len(countries)} country records.")

                # 2. Clean up obsolete child rows for refreshed countries
                delete_languages_sql = "DELETE FROM country_languages WHERE country_code = ANY(%s);"
                delete_currencies_sql = "DELETE FROM country_currencies WHERE country_code = ANY(%s);"

                cur.execute(delete_languages_sql, (country_codes,))
                cur.execute(delete_currencies_sql, (country_codes,))

                # 3. Insert fresh language rows
                if languages:
                    lang_tuples = [
                        (l["country_code"], l["language_code"], l["language_name"], l["native_name"])
                        for l in languages
                    ]
                    cur.executemany(insert_language_sql, lang_tuples)
                    logger.info(f"Loaded {len(lang_tuples)} language records.")

                # 4. Insert fresh currency rows
                if currencies:
                    curr_tuples = [
                        (c["country_code"], c["currency_code"], c["currency_name"], c["symbol"])
                        for c in currencies
                    ]
                    cur.executemany(insert_currency_sql, curr_tuples)
                    logger.info(f"Loaded {len(curr_tuples)} currency records.")

        return {
            "countries_loaded": len(countries),
            "languages_loaded": len(languages),
            "currencies_loaded": len(currencies),
        }
    except Exception as e:
        logger.error(f"Failed to load data into PostgreSQL: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


# ==============================================================================
# PIPELINE ENTRYPOINT
# ==============================================================================

def run_etl(
    api_key: Optional[str] = None,
    db_config: Optional[Dict[str, Any]] = None,
    page_size: int = 100,
) -> Dict[str, Any]:
    """
    Main orchestration entrypoint for running the ETL pipeline end-to-end.
    """
    logger.info("Starting Country Analytics ETL pipeline...")
    raw_data = extract_countries(api_key=api_key, page_size=page_size)
    transformed = transform_dataset(raw_data)
    load_summary = load_data(transformed, db_config=db_config)
    logger.info(f"ETL completed successfully: {load_summary}")
    return load_summary


if __name__ == "__main__":
    try:
        run_etl()
    except Exception as exc:
        logger.error(f"ETL Execution failed: {exc}")
        sys.exit(1)
