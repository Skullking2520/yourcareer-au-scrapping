# google_form_package.py
import os  # noqa
import json
import time
import gspread
from google.oauth2.service_account import Credentials
from selenium import webdriver

class Sheet:
    def __init__(self):
        # This is for GitHub action
        key_content = os.environ.get("SERVICE_ACCOUNT_KEY")
        if not key_content:
            raise FileNotFoundError("Service account key content not found in environment variable!")

        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        credentials = Credentials.from_service_account_info(json.loads(key_content), scopes=scopes)
        gc = gspread.authorize(credentials)
        spreadsheet_url = os.environ.get("SPREADSHEET_URL", "").strip()
        if not spreadsheet_url:
            raise ValueError("Set SPREADSHEET_URL to an authorized test spreadsheet before running.")
        retries = 3
        delay = 5
        for attempt in range(retries):
            try:
                self.spreadsheet = gc.open_by_url(spreadsheet_url)
                break
            except gspread.exceptions.APIError as e:
                if "429" in str(e):
                    print(f"Quota exceeded when opening spreadsheet. Retrying in {delay} seconds (Attempt {attempt+1}/{retries})")
                    time.sleep(delay)
                    delay *= 2
                else:
                    raise
        else:
            raise Exception("Failed to open spreadsheet after multiple attempts due to quota limits.")

    @staticmethod
    def set_driver():
        # set options and driver settings
        user_agent = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
        options = webdriver.ChromeOptions()
        options.add_argument(f"user-agent={user_agent}")
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-extensions")
        options.add_argument('--start-maximized')
        driver = webdriver.Chrome(options=options)
        return driver

    def get_worksheet(self, sheet_name):
        return self.spreadsheet.worksheet(sheet_name)
