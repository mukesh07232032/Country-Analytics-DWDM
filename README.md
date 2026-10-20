# Country Analytics (DWDM)

An end-to-end data warehousing and data mining (DWDM) pipeline that extracts global country demographics, geography, spoken languages, and currencies from the REST Countries v5 API, transforms and validates the raw data into a normalized relational model, orchestrates automated daily ingestion using Apache Airflow, stores analytical tables in PostgreSQL, and enables interactive visual exploration through Metabase and pgAdmin.

---

## 1. Project Title and Overview

- **Project Title:** Country Analytics Data Warehouse & ETL Pipeline
- **Course / Domain:** Data Warehousing and Data Mining (DWDM) / Data Engineering
- **Overview:**  
  Global socio-demographic, geographic, and economic data is often siloed, unstructured, or distributed across dynamic web APIs with varying schemas and rate limits. The Country Analytics project delivers a containerized, production-grade data pipeline that automates the extraction of international country metrics from REST Countries v5, enforces relational integrity, handles real-world edge cases (missing country codes, malformed geometries, division by zero), and loads clean dimensional datasets into PostgreSQL for reporting and business intelligence in Metabase.

---

## 2. Problem Statement

Public REST APIs providing country datasets present several engineering challenges:
1. **API Schema Complexity & Volatility:** REST Countries v5 provides nested JSON payloads containing multilingual names, multi-attribute capital arrays, heterogeneous coordinate formats, and unstructured language and currency mappings.
2. **Missing Identifiers & Inconsistent Metadata:** Certain disputed territories or special entities (e.g., Abkhazia) lack standardized ISO 3166-1 alpha-3 primary keys, leading to primary key violations if ingested blindly.
3. **Derived Demographics & Numeric Hazards:** Key indicators such as population density require dynamic computation ($population / area\_km^2$) subject to zero or null division hazards (e.g., uninhabited territories or micro-dependencies with unrecorded areas).
4. **Relational M-to-N Mappings:** Countries have multiple official languages and multiple circulating currencies. Flat denormalized tables produce massive redundancy and update anomalies.
5. **Orchestration & Idempotency Requirements:** Ingestion pipelines must be capable of running on a schedule without duplicating rows or leaving orphaned child records upon re-execution.

---

## 3. Project Objectives

- **Automated Extraction:** Implement paginated, fault-tolerant extraction from the REST Countries v5 API with token-based authentication and exponential backoff retry logic.
- **Relational Data Modeling:** Design a normalized 3NF star-adjacent schema in PostgreSQL separating core country metrics from language and currency relationships with enforced foreign keys (`ON DELETE CASCADE`).
- **Data Validation & Cleaning:** Systematically validate ISO 3166-1 alpha-3 primary keys, sanitize string bounds, handle division-by-zero risks for population density, and isolate invalid records into structured rejection logs.
- **Idempotent Ingestion:** Ensure all ETL operations support re-runs via atomic database transactions, PostgreSQL `ON CONFLICT DO UPDATE` (upserts), and child-table synchronizations.
- **Workflow Orchestration:** Schedule and monitor end-to-end execution through an Apache Airflow DAG (`country_analytics_pipeline`).
- **Self-Contained Containerization:** Package all services (PostgreSQL, Airflow Webserver, Airflow Scheduler, Metabase, pgAdmin) using Docker Compose on an isolated bridge network with healthchecks and persistent volumes.
- **Business Intelligence & Administration:** Provide out-of-the-box analytical dashboarding with Metabase and web-based database administration with pgAdmin.

---

## 4. Technologies Used

| Layer / Component | Technology | Version / Specification | Role in Architecture |
| :--- | :--- | :--- | :--- |
| **Data Source** | REST Countries API | v5 (`https://api.restcountries.com/countries/v5`) | External JSON REST API |
| **ETL Pipeline** | Python | 3.12 (Standard Library + `psycopg2`) | Ingestion, data cleaning, transformations, and transactional loading |
| **Database Driver** | `psycopg2-binary` | `>=2.9.9` | PostgreSQL connection pool and parameterized SQL execution |
| **Config Loader** | `python-dotenv` | `>=1.0.1` | Local environment variable management (`.env`) |
| **Orchestration** | Apache Airflow | 2.9.3 (`LocalExecutor`, Python 3.12) | DAG scheduling, retries, task logging, and execution monitoring |
| **Database (RDBMS)** | PostgreSQL | 16-alpine | Analytical relational data warehouse and Airflow metadata store |
| **BI & Analytics** | Metabase | Latest official image | Interactive web visualization, aggregations, and dashboards |
| **Database Admin** | pgAdmin 4 | Latest official image | Web GUI for querying tables, viewing indexes, and schema management |
| **Containerization** | Docker & Docker Compose | Compose file format v3.8+ / Compose v2/v5 | Service isolation, healthchecks, volumes, and networking |
| **Testing** | `unittest` / `pytest` | pytest `>=8.0.0` | 100% offline unit tests for extraction, transforms, and edge cases |

> **Note on Pandas:** Pure Python data structures (`dict`, `list`, `set`) and Python standard library modules (`urllib.request`, `json`, `datetime`) were intentionally selected over `pandas` for the core ETL module. This design keeps the Docker image lean, eliminates heavy C-extension compilation overhead, and ensures deterministic type handling and atomic database upserts.

---

## 5. System Architecture and ETL Workflow

```
┌─────────────────────────────────────────────────────────────┐
│                    REST Countries v5 API                    │
│            (Bearer Auth, Paginated JSON Responses)          │
└──────────────────────────────┬──────────────────────────────┘
                               │ HTTP GET (limit=100, fields)
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                       ETL Module (etl.py)                   │
│  1. Extract:    Paged extraction with exponential backoff    │
│  2. Validate:   Check ISO 3166-1 alpha-3 codes & names       │
│  3. Transform:  Primary capitals, pop. density, normalizer   │
│  4. Load:       Atomic transactions, upserts & cascades     │
└──────────────────────────────┬──────────────────────────────┘
                               │
            ┌──────────────────┴──────────────────┐
            ▼                                     ▼
┌───────────────────────┐             ┌───────────────────────┐
│     Apache Airflow    │             │   PostgreSQL 16 DB    │
│  (Scheduler & Web)    │             │ (country_analytics)   │
│  - Scheduled Ingestion│             │  - countries          │
│  - Monitoring & Alert │             │  - country_languages  │
└───────────────────────┘             │  - country_currencies │
                                      └───────────┬───────────┘
                                                  │
                       ┌──────────────────────────┴──────────────────────────┐
                       ▼                                                     ▼
        ┌─────────────────────────────┐                       ┌─────────────────────────────┐
        │          Metabase           │                       │          pgAdmin 4          │
        │  (Analytics & Dashboards)   │                       │   (Database Management)     │
        │  Port: 3000                 │                       │   Port: 5050                │
        └─────────────────────────────┘                       └─────────────────────────────┘
```

### ETL Workflow Steps:
1. **Extraction:**
   - Airflow triggers `run_country_analytics_etl()`.
   - `etl.py` loads credentials securely from the container environment.
   - Iteratively calls `https://api.restcountries.com/countries/v5` using `limit=100` and explicit `response_fields`.
   - Halts pagination when `meta.more == False` or when all records are exhausted.
2. **Transformation & Validation:**
   - Evaluates each record against strict schema rules.
   - Validates that `codes.alpha_3` is non-null and exactly 3 uppercase characters.
   - Extracts primary capital from structured capital objects or falls back to first capital.
   - Calculates `population_density = round(population / area_km2, 4)` with null/zero safeguards.
   - Extracts nested languages into normalized records: `(country_code, language_code, language_name, native_name)`.
   - Extracts nested currencies into normalized records: `(country_code, currency_code, currency_name, symbol)`.
   - Rejects duplicate country codes within the same batch.
3. **Database Ingestion:**
   - Opens an atomic psycopg2 transaction context (`with conn:`).
   - Executes parameterized batch upsert into `countries`.
   - Deletes existing child rows for refreshed country codes to prevent stale or orphaned entries.
   - Inserts fresh child rows into `country_languages` and `country_currencies`.
   - Commits transaction atomically or rolls back on any error.

---

## 6. REST Countries API Integration

- **Base URL:** `https://api.restcountries.com/countries/v5`
- **Authentication:** HTTP Header `Authorization: Bearer <REST_COUNTRIES_API_KEY>`
- **Query Parameters:**
  - `limit`: Number of records per page (bounded between 1 and 100 on the free plan).
  - `offset`: Starting record index for pagination.
  - `response_fields`: Comma-separated field projection to minimize payload size and latency:
    `names.common,names.official,codes.alpha_2,codes.alpha_3,capitals,region,subregion,population,area.kilometers,languages,currencies,coordinates,continents`
- **Response Format:**
  ```json
  {
    "data": {
      "objects": [ ... ],
      "meta": {
        "total": 254,
        "count": 100,
        "offset": 0,
        "limit": 100,
        "more": true
      }
    }
  }
  ```
- **Error Handling & Resilience:**
  - **401 / 403 (Unauthorized / Forbidden):** Immediately raises `PermissionError` without retrying to prevent account lockouts or infinite loops.
  - **429 (Rate Limit) / 5xx (Server Errors) / Network Timeouts:** Automatically retries up to 3 times with exponential backoff (`sleep = 1.5 * attempt`).

---

## 7. Data Extraction, Transformation, Validation, and Loading

### Extraction (`extract_countries`)
- Retrieves records across multiple pages using offset-based pagination.
- Verifies response JSON format, detects API errors, and handles the `_demo` test response mode.

### Transformation & Validation Logic
1. **Country Identifiers (Primary Key):**
   - Extracts `codes.alpha_3` (with fallback to `cca3`).
   - Normalizes to trimmed uppercase string.
   - If missing, empty, or length $\neq 3$, the record is rejected and logged.
2. **Capitals (`transform_capital`):**
   - REST Countries v5 provides capitals as a list of dictionaries with attributes.
   - The transformation searches for `attributes.primary == true`.
   - If not found, falls back to the first non-empty capital string.
   - Returns `None` if no capital data is provided.
3. **Population Density (`transform_population_density`):**
   - Validates that population $\ge 0$ and surface area $> 0$.
   - Computes $density = round(pop / area, 4)$.
   - Returns `None` if area is zero, negative, or missing, preventing arithmetic exceptions.
4. **Geography & Coordinates:**
   - Parses latitude and longitude from nested `coordinates.lat` / `coordinates.lng` or `latlng` arrays.
   - Sanitizes text lengths for `country_name` (max 150), `region` (max 100), and `subregion` (max 100).
5. **Languages & Currencies:**
   - Normalizes heterogeneous API structures (v5 list-of-dicts and legacy key-value maps).
   - Deduplicates codes per country to ensure primary key uniqueness.

### Loading (`load_data`)
- Implements idempotent batch loading using parameterized SQL.
- Protects against database-level deadlocks and duplicate key crashes using `ON CONFLICT (country_code) DO UPDATE`.
- Maintains child table consistency by removing prior mappings for refreshed countries before re-inserting.

---

## 8. Database Schema and Relationships

The schema is defined in [`postgres/init.sql`](postgres/init.sql) and initializes within the `country_analytics` database.

```
┌────────────────────────────────────────────────────────┐
│                      countries                         │
├──────────────────────────────┬─────────────────────────┤
│ country_code (PK)            │ VARCHAR(3)              │
│ alpha2_code                  │ VARCHAR(2)              │
│ country_name                 │ VARCHAR(150) NOT NULL   │
│ official_name                │ TEXT                    │
│ capital                      │ TEXT                    │
│ region                       │ VARCHAR(100)            │
│ subregion                    │ VARCHAR(100)            │
│ population                   │ BIGINT                  │
│ area_km2                     │ NUMERIC                 │
│ latitude                     │ NUMERIC                 │
│ longitude                    │ NUMERIC                 │
│ population_density           │ NUMERIC                 │
│ last_updated                 │ TIMESTAMPTZ             │
└──────────────┬─────────────────────────┬───────────────┘
               │ 1                       │ 1
               │                         │
               │ M                       │ M
┌──────────────▼───────────────┐ ┌───────▼───────────────────────┐
│      country_languages       │ │       country_currencies      │
├──────────────────────────────┤ ├───────────────────────────────┤
│ country_code (PK, FK)        │ │ country_code (PK, FK)         │
│ language_code (PK)           │ │ currency_code (PK)            │
│ language_name                │ │ currency_name                 │
│ native_name                  │ │ symbol                        │
└──────────────────────────────┘ └───────────────────────────────┘
```

### 1. `countries` (Dimension Table)
- **Primary Key:** `country_code` (`VARCHAR(3)`) — ISO 3166-1 alpha-3.
- **Attributes:** Common name, official name, primary capital, region, subregion, population count, area in $\text{km}^2$, geographic coordinates (lat/long), computed population density, and ingestion timestamp (`last_updated`).

### 2. `country_languages` (Bridge Table - 1:M)
- **Primary Key:** Composite `(country_code, language_code)`.
- **Foreign Key:** `country_code` references `countries(country_code)` with `ON DELETE CASCADE`.
- **Attributes:** ISO/BCP47 language code, English language name, native name.

### 3. `country_currencies` (Bridge Table - 1:M)
- **Primary Key:** Composite `(country_code, currency_code)`.
- **Foreign Key:** `country_code` references `countries(country_code)` with `ON DELETE CASCADE`.
- **Attributes:** ISO 4217 currency code, currency name, currency symbol.

### Performance Indexes
- `idx_countries_region` on `countries(region)`
- `idx_countries_subregion` on `countries(subregion)`
- `idx_countries_population` on `countries(population)`
- `idx_countries_area_km2` on `countries(area_km2)`
- `idx_countries_population_density` on `countries(population_density)`

---

## 9. Project Directory Structure

```
.
├── .env.example               # Sanitized template for environment variables
├── .gitignore                  # Git exclusion rules (ignores .env, logs, pycache)
├── Dockerfile                  # Custom Airflow container image with dependencies
├── README.md                   # Comprehensive project documentation
├── api_test.py                 # Standalone script to test API connectivity & key
├── dags/
│   └── country_analytics_dag.py # Apache Airflow DAG definition
├── docker-compose.yml          # Container service definitions, networks, and volumes
├── etl.py                      # Core Python ETL module (Extract, Transform, Load)
├── pgadmin/
│   └── servers.json            # pgAdmin automatic PostgreSQL connection import
├── postgres/
│   └── init.sql                # PostgreSQL DDL initialization schema & indexes
├── pytest.ini                  # Pytest configuration with pythonpath = .
├── requirements.txt            # Python dependencies for ETL and testing
└── tests/
    └── test_transformations.py # Comprehensive offline unit test suite (13 tests)
```

---

## 10. Prerequisites

Before running the project, ensure your host system has the following installed:
- **Docker Engine:** Version 24.0 or later
- **Docker Compose:** Version 2.20 or later (or Docker Desktop on macOS/Windows/Linux)
- **Python:** 3.12 or later (if executing unit tests or scripts locally outside Docker)
- **REST Countries API Key:** Free tier account key obtained from [REST Countries](https://restcountries.com/).

---

## 11. Environment Configuration

All environment-specific variables and secrets are managed through a local `.env` file. 

1. **Create `.env` from template:**
   ```bash
   cp .env.example .env
   ```

2. **Configure your variables in `.env`:**
   Open `.env` in an editor and populate your credentials:

   ```ini
   # REST Countries API Configuration
   REST_COUNTRIES_API_KEY=your_actual_api_key_here

   # PostgreSQL Database Configuration
   POSTGRES_USER=airflow
   POSTGRES_PASSWORD=choose_a_secure_postgres_password
   POSTGRES_DB=country_analytics
   POSTGRES_PORT=5432

   # Apache Airflow Configuration
   AIRFLOW_IMAGE_NAME=country-analytics-airflow:2.9.3-python3.12
   AIRFLOW_UID=50000
   AIRFLOW_PROJ_DIR=.
   AIRFLOW_WEBSERVER_PORT=8081
   _AIRFLOW_WWW_USER_USERNAME=admin
   _AIRFLOW_WWW_USER_PASSWORD=admin
   _AIRFLOW_WWW_USER_EMAIL=admin@example.com
   _AIRFLOW_WWW_USER_FIRSTNAME=Airflow
   _AIRFLOW_WWW_USER_LASTNAME=Admin

   # pgAdmin Configuration
   PGADMIN_DEFAULT_EMAIL=admin@example.com
   PGADMIN_DEFAULT_PASSWORD=choose_a_secure_pgadmin_password
   PGADMIN_PORT=5050

   # Metabase Configuration
   METABASE_PORT=3000
   ```

> **Security Warning:** Never commit `.env` to version control. The `.gitignore` file is pre-configured to exclude `.env` and `*.env.local`.

---

## 12. Installation and Startup Instructions

1. **Verify API Connectivity (Optional, Local):**
   You can verify your API key using the standalone verification script:
   ```bash
   python3 api_test.py
   ```
   *(Prints HTTP status, pagination info, and sample records without exposing secrets).*

2. **Build and Start All Docker Services:**
   Execute from the project root:
   ```bash
   docker compose up -d --build
   ```

3. **Verify Service Health and Status:**
   ```bash
   docker compose ps
   ```
   Expected running containers:
   - `country_analytics_postgres` (Port `5432`, Healthy)
   - `country_analytics_airflow_init` (Exited with code 0)
   - `country_analytics_airflow_scheduler` (Healthy)
   - `country_analytics_airflow_webserver` (Port `8081`, Healthy)
   - `country_analytics_metabase` (Port `3000`, Up)
   - `country_analytics_pgadmin` (Port `5050`, Up)

4. **Shutting Down the Environment:**
   ```bash
   docker compose down
   ```
   *(To wipe persistent database volumes, use `docker compose down -v`)*.

---

## 13. Airflow DAG Execution and Monitoring

- **Webserver URL:** `http://localhost:8081`
- **Default Login:**
  - **Username:** `admin` (or configured value in `.env`)
  - **Password:** `admin` (or configured value in `.env`)

### DAG Characteristics:
- **DAG ID:** `country_analytics_pipeline`
- **Schedule:** `@daily` (runs every midnight UTC)
- **Catchup:** `False`
- **Retries:** 2 retries with a 5-minute retry delay

### Triggering Ingestion Manually:
1. Navigate to `http://localhost:8081` in your browser.
2. Locate `country_analytics_pipeline` in the DAG list.
3. Unpause the DAG using the toggle switch.
4. Click the **Trigger DAG** button (Play icon) on the top right.
5. Click into **Graph** or **Grid** view to monitor task `extract_transform_load_countries`.
6. Click the task box and select **Log** to inspect ingestion metrics (records extracted, countries upserted, rejected count).

---

## 14. PostgreSQL Connection Information

PostgreSQL can be accessed directly from your host or from other containers on the Docker network:

| Parameter | Host Machine Access | Container / Docker Network Access |
| :--- | :--- | :--- |
| **Host** | `localhost` or `127.0.0.1` | `postgres` (Service Name) |
| **Port** | `5432` | `5432` |
| **Database** | `country_analytics` | `country_analytics` |
| **Username** | `airflow` | `airflow` |
| **Password** | Value of `POSTGRES_PASSWORD` from `.env` | Value of `POSTGRES_PASSWORD` from `.env` |

### CLI Inspection Example:
```bash
docker compose exec postgres psql -U airflow -d country_analytics -c "SELECT count(*) FROM countries;"
```

---

## 15. Metabase Database Configuration and Dashboard Usage

Metabase provides business intelligence dashboards and analytical visualizations over the ingested country warehouse.

- **Web URL:** `http://localhost:3000`

### Initial Setup & Database Connection:
1. Open `http://localhost:3000` in your web browser and complete the one-time administrator account creation.
2. In the setup wizard (or via **Settings > Admin settings > Databases > Add database**), select **PostgreSQL**.
3. Fill in the connection form using the internal Docker service values:
   - **Database type:** `PostgreSQL`
   - **Display name:** `Country Analytics`
   - **Host:** `postgres` *(Must be the Docker container hostname, not localhost)*
   - **Port:** `5432`
   - **Database name:** `country_analytics`
   - **Database username:** `airflow`
   - **Database password:** *(Your `POSTGRES_PASSWORD` configured in `.env`)*
4. Click **Save** to trigger Metabase schema synchronization.

### Recommended Analytical Dashboards & Visualizations:
- **World Population by Region:** Bar or donut chart grouping `population` by `region`.
- **Top 10 Most Densely Populated Countries:** Table or horizontal bar chart sorting by `population_density DESC`.
- **Global Map Distribution:** Pin map using `latitude` and `longitude` with bubble size proportional to `population`.
- **Most Widely Spoken Languages:** Bar chart aggregating `count(country_code)` from `country_languages` grouped by `language_name`.
- **Currency Usage Distribution:** Pie chart of top currencies from `country_currencies`.

---

## 16. pgAdmin Access

pgAdmin provides full database management, SQL query execution, and visual schema inspection.

- **Web URL:** `http://localhost:5050`
- **Login Credentials:**
  - **Email:** `admin@example.com` (or value of `PGADMIN_DEFAULT_EMAIL` in `.env`)
  - **Password:** Value of `PGADMIN_DEFAULT_PASSWORD` in `.env`

### Pre-Configured Server Connection:
- The PostgreSQL server is automatically pre-configured on startup via [`pgadmin/servers.json`](pgadmin/servers.json).
- In the left sidebar under **Servers**, expand `PostgreSQL (Country Analytics)`.
- When prompted for the database password, enter your `POSTGRES_PASSWORD`.
- Browse to **Databases > country_analytics > Schemas > public > Tables** to view `countries`, `country_languages`, and `country_currencies`.

---

## 17. Running Unit Tests

The test suite in [`tests/test_transformations.py`](tests/test_transformations.py) is 100% offline, requires no network calls, and does not require running database containers.

### Running Tests with Pytest:
```bash
pytest tests/test_transformations.py
```
*(Or simply `pytest` from the root; `pytest.ini` automatically sets the python path)*.

### Running Tests with Python Unittest:
```bash
python3 -m unittest discover tests
```

### Coverage of Test Cases (13 Tests):
1. `test_transform_valid_country`: Verifies complete field mapping, data types, and population density rounding.
2. `test_transform_missing_optional_fields`: Validates resilience when optional attributes (capital, region, population) are null.
3. `test_population_density_zero_or_missing_area`: Ensures division-by-zero protection when area is 0, negative, or None.
4. `test_transform_multiple_languages`: Verifies normalization and deduplication of language mappings.
5. `test_transform_multiple_currencies`: Verifies currency code uppercase standardization and deduplication.
6. `test_transform_capital_selection`: Confirms selection of `primary=True` capitals and fallback handling.
7. `test_transform_malformed_records`: Asserts rejection of non-dictionary payloads, missing keys, and invalid-length alpha-3 codes.
8. `test_transform_invalid_population_and_coordinates`: Tests non-integer populations and non-numeric coordinates.
9. `test_transform_dataset_batch`: Validates batch processing, isolation of rejected records (e.g., Abkhazia), and in-batch deduplication.
10. `test_mask_secret`: Confirms safe masking of sensitive tokens (`1234...cdef` and `***`).
11. `test_get_api_key_missing_raises_error`: Validates `ValueError` when API key is missing.
12. `test_fetch_page_success`: Validates mocked HTTP request and JSON parsing.
13. `test_extract_countries_stops_on_more_false`: Verifies pagination termination when `meta.more == False`.

---

## 18. Data Quality and Validation

The pipeline enforces data quality across three stages:
1. **Source Filtering:** Only records with valid, 3-letter ISO 3166-1 alpha-3 identifiers are admitted into the primary dimension table. Records without valid alpha-3 codes (such as Abkhazia in the v5 API) are cleanly rejected and logged.
2. **Numeric Integrity:** Calculations for derived metrics (`population_density`) check bounds ($pop \ge 0, area > 0$). Negative populations or invalid non-numeric coordinates are coerced to `None` (NULL in SQL).
3. **Relational Consistency:** 
   - Child records in `country_languages` and `country_currencies` cannot exist without a valid parent in `countries`.
   - Foreign key constraints with `ON DELETE CASCADE` guarantee referential integrity.
   - Child records are refreshed on every ingestion run to eliminate stale or obsolete language and currency mappings.

---

## 19. Error Handling and Security

- **Credential Separation:** No hardcoded API keys or passwords exist in the codebase. All secrets are passed via environment variables loaded from `.env`.
- **Log Sanitation:** The `mask_secret()` function masks credentials in all application logs (displaying only the first 4 and last 4 characters).
- **Transient Failure Resilience:** HTTP requests implement exponential backoff retry logic for network timeouts and transient HTTP status codes (429, 500, 502, 503, 504).
- **SQL Injection Prevention:** All SQL queries in `etl.py` use parameterized queries (`%s` and `%(key)s`), completely preventing SQL injection.
- **Port Conflict Protection:** The Airflow webserver host port is explicitly decoupled to `8081` to prevent binding conflicts with pgAdmin or local services on port `8080`.

---

## 20. Limitations and Future Enhancements

### Current Limitations:
- **Free Tier API Rate Limits:** The REST Countries v5 free tier limits page size to 100 records and imposes hourly request limits.
- **Single-Source Scope:** The data warehouse currently ingests only geographical and demographic indicators from REST Countries without cross-referencing external economic or historical datasets.
- **Full Refresh Child Synchronization:** While the parent `countries` table is updated via incremental upsert, child language and currency records are replaced per refreshed country.

### Future Enhancements:
- **Historical SCD Type 2 Tracking:** Implement Slowly Changing Dimensions (SCD Type 2) in PostgreSQL to track demographic shifts (e.g., population growth over decades) with `effective_date` and `is_current` columns.
- **Additional Data Sources:** Integrate World Bank and UN data APIs for time-series economic indicators (GDP, inflation, life expectancy) linked via `country_code`.
- **Automated Data Quality Testing:** Integrate Great Expectations or dbt (data build tool) into the Airflow DAG to automate schema validation and assertion tests after each pipeline execution.
