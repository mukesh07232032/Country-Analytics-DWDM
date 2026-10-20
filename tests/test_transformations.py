"""
Unit test suite for Country Analytics ETL transformation and extraction logic.
Runs completely offline without requiring PostgreSQL or external network connections.
"""

import json
import os
import unittest
from unittest.mock import MagicMock, patch

from etl import (
    extract_countries,
    fetch_page,
    get_api_key,
    mask_secret,
    transform_capital,
    transform_country,
    transform_currencies,
    transform_dataset,
    transform_languages,
    transform_population_density,
)


class TestETLTransformations(unittest.TestCase):
    def setUp(self):
        # Sample realistic REST Countries v5 record (Canada)
        self.sample_v5_country = {
            "names": {
                "common": "Canada",
                "official": "Canada",
            },
            "codes": {
                "alpha_2": "CA",
                "alpha_3": "CAN",
            },
            "capitals": [
                {
                    "name": "Ottawa",
                    "coordinates": {"lat": 45.42, "lng": -75.7},
                    "attributes": {
                        "primary": True,
                        "administrative": False,
                        "constitutional": False,
                        "executive": False,
                        "judicial": False,
                        "legislative": False,
                    },
                }
            ],
            "region": "Americas",
            "subregion": "North America",
            "population": 41798407,
            "area": {"kilometers": 9984670.0, "miles": 3855100.0},
            "coordinates": {"lat": 60.0, "lng": -95.0},
            "continents": ["North America"],
            "languages": [
                {
                    "bcp47": "en",
                    "iso639_1": "en",
                    "iso639_2b": "eng",
                    "iso639_2t": "eng",
                    "iso639_3": "eng",
                    "name": "English",
                    "native_name": "English",
                },
                {
                    "bcp47": "fr",
                    "iso639_1": "fr",
                    "iso639_2b": "fre",
                    "iso639_2t": "fra",
                    "iso639_3": "fra",
                    "name": "French",
                    "native_name": "français",
                },
            ],
            "currencies": [
                {
                    "code": "CAD",
                    "name": "Canadian dollar",
                    "symbol": "$",
                }
            ],
        }

    # --------------------------------------------------------------------------
    # 1. Valid Country Record Transformation
    # --------------------------------------------------------------------------
    def test_transform_valid_country(self):
        result = transform_country(self.sample_v5_country)
        self.assertIsNotNone(result)
        self.assertEqual(result["country_code"], "CAN")
        self.assertEqual(result["alpha2_code"], "CA")
        self.assertEqual(result["country_name"], "Canada")
        self.assertEqual(result["official_name"], "Canada")
        self.assertEqual(result["capital"], "Ottawa")
        self.assertEqual(result["region"], "Americas")
        self.assertEqual(result["subregion"], "North America")
        self.assertEqual(result["population"], 41798407)
        self.assertEqual(result["area_km2"], 9984670.0)
        self.assertEqual(result["latitude"], 60.0)
        self.assertEqual(result["longitude"], -95.0)
        self.assertAlmostEqual(result["population_density"], 4.1863, places=4)
        self.assertIsNotNone(result["last_updated"])

    # --------------------------------------------------------------------------
    # 2. Missing Optional Fields Handling
    # --------------------------------------------------------------------------
    def test_transform_missing_optional_fields(self):
        minimal_country = {
            "codes": {"alpha_3": "ISL"},
            "names": {"common": "Iceland"},
        }
        result = transform_country(minimal_country)
        self.assertIsNotNone(result)
        self.assertEqual(result["country_code"], "ISL")
        self.assertEqual(result["country_name"], "Iceland")
        self.assertIsNone(result["alpha2_code"])
        self.assertIsNone(result["official_name"])
        self.assertIsNone(result["capital"])
        self.assertIsNone(result["region"])
        self.assertIsNone(result["subregion"])
        self.assertIsNone(result["population"])
        self.assertIsNone(result["area_km2"])
        self.assertIsNone(result["latitude"])
        self.assertIsNone(result["longitude"])
        self.assertIsNone(result["population_density"])

    # --------------------------------------------------------------------------
    # 3. Missing or Zero Area Handling (Division by Zero Prevention)
    # --------------------------------------------------------------------------
    def test_population_density_zero_or_missing_area(self):
        # Missing area
        self.assertIsNone(transform_population_density(1000000, None))
        # Zero area
        self.assertIsNone(transform_population_density(1000000, 0))
        # Negative area
        self.assertIsNone(transform_population_density(1000000, -50))
        # Missing population
        self.assertIsNone(transform_population_density(None, 50000))
        # Negative population
        self.assertIsNone(transform_population_density(-100, 50000))
        # Valid calculation
        self.assertEqual(transform_population_density(1000, 200), 5.0)

    # --------------------------------------------------------------------------
    # 4. Multiple Languages Extraction & Deduplication
    # --------------------------------------------------------------------------
    def test_transform_multiple_languages(self):
        langs = transform_languages("CAN", self.sample_v5_country["languages"])
        self.assertEqual(len(langs), 2)
        codes = [l["language_code"] for l in langs]
        self.assertIn("en", codes)
        self.assertIn("fr", codes)
        self.assertEqual(langs[0]["country_code"], "CAN")

        # Test deduplication and skipping invalid items
        raw_with_dupes = [
            {"bcp47": "en", "name": "English", "native_name": "English"},
            {"bcp47": "en", "name": "English", "native_name": "English Duplicate"},
            {"bcp47": None, "name": "Missing Code"},
            "invalid_non_dict",
        ]
        cleaned = transform_languages("XYZ", raw_with_dupes)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]["language_code"], "en")

    # --------------------------------------------------------------------------
    # 5. Multiple Currencies Extraction & Deduplication
    # --------------------------------------------------------------------------
    def test_transform_multiple_currencies(self):
        raw_currencies = [
            {"code": "EUR", "name": "Euro", "symbol": "€"},
            {"code": "CHF", "name": "Swiss Franc", "symbol": "CHF"},
            {"code": "EUR", "name": "Duplicate Euro", "symbol": "€"},
            {"code": None, "name": "Invalid Currency"},
        ]
        currencies = transform_currencies("CHE", raw_currencies)
        self.assertEqual(len(currencies), 2)
        codes = [c["currency_code"] for c in currencies]
        self.assertEqual(codes, ["EUR", "CHF"])

    # --------------------------------------------------------------------------
    # 6. Structured Capital Selection (Primary vs First) & Missing Capital
    # --------------------------------------------------------------------------
    def test_transform_capital_selection(self):
        # Primary identified
        capitals_multiple = [
            {"name": "Secondary City", "attributes": {"primary": False}},
            {"name": "Pretoria", "attributes": {"primary": True}},
            {"name": "Cape Town", "attributes": {"primary": False}},
        ]
        self.assertEqual(transform_capital(capitals_multiple), "Pretoria")

        # No primary flag, takes first valid
        capitals_no_primary = [
            {"name": "First Capital"},
            {"name": "Second Capital"},
        ]
        self.assertEqual(transform_capital(capitals_no_primary), "First Capital")

        # Empty / None
        self.assertIsNone(transform_capital([]))
        self.assertIsNone(transform_capital(None))
        self.assertIsNone(transform_capital([{"name": ""}]))

    # --------------------------------------------------------------------------
    # 7. Malformed Records Handling
    # --------------------------------------------------------------------------
    def test_transform_malformed_records(self):
        # Non-dict record
        self.assertIsNone(transform_country("not a dictionary"))  # type: ignore

        # Missing country code
        no_code = {"names": {"common": "NoCodeLand"}}
        self.assertIsNone(transform_country(no_code))

        # Country code not 3 characters
        invalid_code_len = {
            "codes": {"alpha_3": "TOOLONG"},
            "names": {"common": "Invalid"},
        }
        self.assertIsNone(transform_country(invalid_code_len))

        # Missing common country name
        no_name = {"codes": {"alpha_3": "XYZ"}}
        self.assertIsNone(transform_country(no_name))

    # --------------------------------------------------------------------------
    # 8. Invalid Population & Coordinate Values
    # --------------------------------------------------------------------------
    def test_transform_invalid_population_and_coordinates(self):
        bad_numbers = {
            "codes": {"alpha_3": "TST"},
            "names": {"common": "TestLand"},
            "population": "not_an_int",
            "coordinates": {"lat": "invalid_lat", "lng": None},
        }
        res = transform_country(bad_numbers)
        self.assertIsNotNone(res)
        self.assertIsNone(res["population"])
        self.assertIsNone(res["latitude"])
        self.assertIsNone(res["longitude"])
        self.assertIsNone(res["population_density"])

    # --------------------------------------------------------------------------
    # 9. Entire Dataset Transformation
    # --------------------------------------------------------------------------
    def test_transform_dataset_batch(self):
        abkhazia_mock = {
            "names": {"common": "Abkhazia"},
            "codes": {"alpha_2": "", "alpha_3": ""},
        }
        dataset = [
            self.sample_v5_country,
            # Duplicate country code should be rejected/ignored
            self.sample_v5_country,
            # Malformed item
            {"invalid": True},
            # Missing alpha-3 code (e.g. Abkhazia)
            abkhazia_mock,
        ]
        batch = transform_dataset(dataset)
        self.assertEqual(len(batch["countries"]), 1)
        self.assertEqual(len(batch["languages"]), 2)
        self.assertEqual(len(batch["currencies"]), 1)
        self.assertEqual(len(batch["rejected_records"]), 3)
        rejected_names = [r["country_name"] for r in batch["rejected_records"]]
        self.assertIn("Canada", rejected_names)  # Duplicate
        self.assertIn("Abkhazia", rejected_names)  # Missing alpha-3
        self.assertIn("Unknown", rejected_names)  # Malformed item

    # --------------------------------------------------------------------------
    # 10. Configuration & Secret Masking
    # --------------------------------------------------------------------------
    def test_mask_secret(self):
        self.assertEqual(mask_secret("1234567890abcdef"), "1234...cdef")
        self.assertEqual(mask_secret("short"), "***")
        self.assertEqual(mask_secret(None), "<EMPTY>")

    @patch("etl.load_environment")
    @patch.dict(os.environ, {}, clear=True)
    def test_get_api_key_missing_raises_error(self, mock_load_env):
        with self.assertRaises(ValueError) as ctx:
            get_api_key()
        self.assertIn("REST_COUNTRIES_API_KEY is not set", str(ctx.exception))

    # --------------------------------------------------------------------------
    # 11. Extraction Mocking (Offline Verification)
    # --------------------------------------------------------------------------
    @patch("urllib.request.urlopen")
    def test_fetch_page_success(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "data": {
                "objects": [self.sample_v5_country],
                "meta": {"total": 1, "count": 1, "more": False},
            }
        }).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_response

        objects, meta, is_demo = fetch_page(api_key="mock_key", limit=10, offset=0)
        self.assertEqual(len(objects), 1)
        self.assertEqual(objects[0]["names"]["common"], "Canada")
        self.assertFalse(meta.get("more"))
        self.assertFalse(is_demo)

    @patch("urllib.request.urlopen")
    def test_extract_countries_stops_on_more_false(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "data": {
                "objects": [self.sample_v5_country],
                "meta": {"total": 1, "count": 1, "more": False},
            }
        }).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_response

        records = extract_countries(api_key="mock_key", page_size=10)
        self.assertEqual(len(records), 1)
        # Verify it made only 1 request because more=False
        self.assertEqual(mock_urlopen.call_count, 1)


if __name__ == "__main__":
    unittest.main()
