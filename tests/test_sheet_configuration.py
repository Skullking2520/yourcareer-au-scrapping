import os
import unittest
from unittest.mock import patch

from scripts import google_form_package as sheets


class SheetConfigurationTests(unittest.TestCase):
    def test_authorized_target_is_passed_without_creating_a_key_file(self):
        url = "https://docs.google.com/spreadsheets/d/TEST_ONLY/edit"
        with (
            patch.dict(os.environ, {"SERVICE_ACCOUNT_KEY": "{}", "SPREADSHEET_URL": url}, clear=True),
            patch.object(sheets.Credentials, "from_service_account_info"),
            patch.object(sheets.gspread, "authorize") as authorize,
            patch.object(sheets.webdriver, "Chrome") as browser,
            patch("builtins.open", side_effect=AssertionError("Unexpected credential file write")),
        ):
            sheets.Sheet()
            authorize.return_value.open_by_url.assert_called_once_with(url)
            browser.assert_not_called()

    def test_invalid_configuration_stops_before_authentication(self):
        url = "https://docs.google.com/spreadsheets/d/TEST_ONLY/edit"
        cases = [
            {},
            {"SERVICE_ACCOUNT_KEY": "{}"},
            {"SERVICE_ACCOUNT_KEY": "{}", "SPREADSHEET_URL": "https://example.invalid/test"},
            {"SERVICE_ACCOUNT_KEY": "PRIVATE_TEST_CONTENT", "SPREADSHEET_URL": url},
            {"SERVICE_ACCOUNT_KEY": "[]", "SPREADSHEET_URL": url},
        ]
        for env in cases:
            with self.subTest(fields=list(env)):
                with (
                    patch.dict(os.environ, env, clear=True),
                    patch.object(sheets.Credentials, "from_service_account_info") as credentials,
                    patch.object(sheets.gspread, "authorize") as authorize,
                ):
                    with self.assertRaises(ValueError) as error:
                        sheets.Sheet()
                    self.assertNotIn("PRIVATE_TEST_CONTENT", str(error.exception))
                    credentials.assert_not_called()
                    authorize.assert_not_called()


if __name__ == "__main__":
    unittest.main()
