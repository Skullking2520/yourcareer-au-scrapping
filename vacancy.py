import json
from urllib.parse import urljoin  # join url
from requests import ReadTimeout
from selenium import webdriver  # web scrapping
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.oauth2.service_account import Credentials  # google doc
import time
import os
import re
import gspread
import datetime

key_content = os.environ.get("SERVICE_ACCOUNT_KEY")
if not key_content:
    raise FileNotFoundError("Service account key content not found in environment variable!")

key_path = "service_account.json"
with open(key_path, "w") as f:
    f.write(key_content)

scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
spreadsheet_url = "https://docs.google.com/spreadsheets/d/13fIG9eUVVH1OKkQ6CaaTNSr1Cb8eUg-qCNXxm9m7eu0/edit?gid=0#gid=0"
credentials = Credentials.from_service_account_file(key_path, scopes=scopes)
gc = gspread.authorize(credentials)
spreadsheet = gc.open_by_url(spreadsheet_url)
URL = "https://www.workforceaustralia.gov.au/individuals/jobs/search?locationCodes%5B0%5D=7&pageNumber="
BATCH_SIZE = 10

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

class ProgressManager:
    def __init__(self, progress_sheet):
        self.progress_sheet = progress_sheet
        self._cached_progress = None

    def _is_first_execution(self):
        if self._cached_progress is not None:
            return not bool(self._cached_progress.strip())
        try:
            progress_json = self.progress_sheet.acell("A2").value
            return not bool(progress_json)
        except Exception:
            return True

    def save_progress(self, progress):
        self._cached_progress = json.dumps(progress)
        if self._is_first_execution():
            progress = {
                "Phase": "Scrapping",
                "finished": False,
                "UrlNum": 0,
            }
        try:
            self.progress_sheet.update("A2", json.dumps(progress))
        except Exception:
            print("Failed to save progress.")

    def load_progress(self):
        if self._cached_progress is not None:
            try:
                return json.loads(self._cached_progress)
            except Exception:
                pass
        try:
            if self._is_first_execution():
                progress = {
                    "Phase": "Scrapping",
                    "finished": False,
                    "UrlNum": 0,
                }
                return progress
            else:
                progress_json = self.progress_sheet.acell("A2").value
                if progress_json:
                    progress = json.loads(progress_json)
                    return progress
                else:
                    raise Exception("No progress value found in A2")
        except Exception:
            print("Failed to load progress, finishing program")
            return {"finished": True}

def get_worksheet(sheet_name):
    return spreadsheet.worksheet(sheet_name)

def set_vacancy_sheet():
    # set for vacancy sheet
    worksheet = get_worksheet("Vacancies")
    worksheet.clear()
    headers = ["occupation", "occupation link", "date added", "time scrapped", "job title", "job link", "job code", "company", "salary",
               "address", "lat", "long", "tenure", "overview", "closes", "description"]
    worksheet.append_row(headers)
    return worksheet

def set_vacancy_data_sheet():
    # set for VacancyData sheet
    worksheet = get_worksheet("VacancyData")
    worksheet.clear()
    headers = ["job_title","job_link","job_code", "date_added","time_scrapped","overview"]
    worksheet.append_row(headers)
    return worksheet

def append_row_with_retry(worksheet, data, retries=3, delay=5):
    for attempt in range(retries):
        try:
            worksheet.append_row(data, value_input_option="USER_ENTERED")
            return
        except gspread.exceptions.APIError as e:
            if any(code in str(e) for code in ["500", "502", "503", "504"]) or isinstance(e, ReadTimeout):
                print(f"Error occurred. Retry after {delay} seconds ({attempt+1}/{retries})")
                time.sleep(delay)
            else:
                print(f"Failed to append element {data} after {retries} attempts.")
                return

def is_first_execution(progress_sheet):
    progress_value = progress_sheet.acell("A2").value
    return not progress_value or progress_value.strip() == ""

def get_vacancy_data():
    sheet = get_worksheet("VacancyData")
    all_rows = sheet.get_all_values()
    if not all_rows:
        return []

    header = all_rows[0]
    vacancy_list = []
    for row in all_rows[1:]:
        row_dict = {header[i]: row[i] if i < len(row) else "" for i in range(len(header))}
        vacancy_list.append(row_dict)
    return vacancy_list

def duplicate_list():
    va_sheet = get_worksheet("Vacancies")
    va_header = va_sheet.row_values(1)
    try:
        va_code_idx = va_header.index("job code") + 1
    except ValueError as e:
        print("Could not detect requested row", e)
        return

    all_rows = va_sheet.get_all_values()[1:]
    dup_list = []

    for row_num, row in enumerate(all_rows, start=2):
        va_code = row[va_code_idx - 1] if len(row) >= va_code_idx else ""
        dup_list.append(va_code)
    return dup_list

def dict_to_row(data_dict):
    return [data_dict.get(key, "") for key in ["job_title","job_link","job_code", "date_added","time_scrapped","overview"]]

def wait_for_page_load(driver, timeout=15):
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException:
        print("Page loading timeout.")
    except Exception as e:
        print(f"An error occurred while waiting for page load: {e}")

def scrapping(driver):
    wait = WebDriverWait(driver, 10)
    data_sheet = get_worksheet("VacancyData")

    progress_sheet = get_worksheet("Progress")
    if is_first_execution(progress_sheet):
        set_vacancy_sheet()
    progress_manager = ProgressManager(progress_sheet)
    progress = progress_manager.load_progress()
    url_num = progress.get("UrlNum", 1)
    while True:
        va_url = URL + str(url_num)

        try:
            driver.get(va_url)
        except Exception:
            print(f"Failed to load page: {va_url}")
            url_num += 1
            continue

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        wait_for_page_load(driver)

        vac_index = 0 # reset VacIndex for this OccIndex
        set_vacancy_data_sheet()

        while True:
            try:
                vacancies = wait.until(EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "section.mint-search-result-item.has-img.has-actions.has-preheading")))
            except TimeoutException:
                print(f"Vacancy elements for page {url_num} did not load in time.")
                break
            except Exception as e:
                print(f"An error occurred while waiting for page load: {e}")
                break

            vac_index = 0

            while vac_index < len(vacancies):
                # find job code, update occupation index
                vacancy = vacancies[vac_index]
                # find job title
                try:
                    base_url = "https://www.workforceaustralia.gov.au"
                    job_hyper = vacancy.find_element(By.CSS_SELECTOR, "a[class='mint-link link']")
                    job_title = job_hyper.text
                    job_href = job_hyper.get_attribute("href")
                    job_link = urljoin(base_url, job_href)
                    job_code = job_href.split('/')[-1]
                except NoSuchElementException:
                    job_title = "No job title given"
                    job_link = "No job link given"
                    job_code = "No job code given"

                try:
                    raw_date_added_dif = vacancy.find_element(By.CSS_SELECTOR, "div[class='preheading']").text
                    match = re.search(r'\d+', raw_date_added_dif)
                    if match:
                        date_added_dif = int(match.group())
                    else:
                        date_added_dif = 1
                    today = datetime.date.today()
                    date_added = today - datetime.timedelta(days=date_added_dif)
                except NoSuchElementException:
                    date_added = "No date added given"

                time_scrapped = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

                try:
                    overview = vacancy.find_element(By.CSS_SELECTOR, "span[class='mint-blurb__text-width']").text
                except NoSuchElementException:
                    overview = "No overview given"

                vacancy_data = {
                    "job_title": job_title,
                    "job_link": job_link,
                    "job_code": job_code,
                    "date_added": str(date_added),
                    "time_scrapped": str(time_scrapped),
                    "overview": overview
                }
                vacancy_row = dict_to_row(vacancy_data)

                append_row_with_retry(data_sheet, vacancy_row)
                time.sleep(3)
                vac_index += 1

            try:
                next_button = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "button[aria-label='Go to next page']")))
                driver.execute_script("arguments[0].click();", next_button)
                wait_for_page_load(driver)
                progress = {"Phase": "Scrapping", "finished": False, "UrlNum": url_num}
                progress_manager.save_progress(progress)
            except (NoSuchElementException, TimeoutException):
                progress = {"Phase": "Detail", "finished": False, "UrlNum": 1}
                progress_manager.save_progress(progress)
                break
            except Exception as e:
                print(f"An error occurred while finding next button: {e}")
                break

def detail(driver):
    va_sheet = get_worksheet("Vacancies")
    progress_sheet = get_worksheet("Progress")
    if is_first_execution(progress_sheet):
        set_vacancy_sheet()
    progress_manager = ProgressManager(progress_sheet)
    progress = progress_manager.load_progress()
    url_num = progress.get("UrlNum", 1)

    vac_index = 0  # reset VacIndex for this OccIndex
    vacancy_data = get_vacancy_data()
    seen_jobs = set(duplicate_list() or [])
    while vac_index < len(vacancy_data):
        vac_element = vacancy_data[vac_index]
        # open detail page
        driver.get(vac_element["job_link"])
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        wait_for_page_load(driver)
        print(f"current page: {vac_element['job_title']}")

        try:
            company_elem = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located(
                    (By.XPATH, "//*[@id='find-a-job']//div[contains(@class, 'text-lg')]//p/a")
                )
            )
            company = company_elem.text
        except (NoSuchElementException, TimeoutException):
            company = "No company given"
        except Exception as e:
            print(f"An error occurred while finding company data: {e}")
            break

        if vac_element['job_code'] in seen_jobs:
            print(f"Duplicate found, skipping: {company}, {vac_element['job_title']}")
            vac_index += 1
            continue

        try:
            address = driver.find_element(By.CSS_SELECTOR, "div[class='address-text']").text
        except NoSuchElementException:
            address = "No address given"

        try:
            salary = driver.find_element(
                By.CSS_SELECTOR,
                "ul.job-info-metadata > li:nth-child(2) > span:nth-of-type(2)"
            ).text
        except NoSuchElementException:
            salary = "No salary given"

        try:
            tenure = driver.find_element(
                By.CSS_SELECTOR,
                "ul.job-info-metadata > li:nth-child(3) > span:nth-of-type(2)"
            ).text
        except NoSuchElementException:
            tenure = "No tenure given"

        try:
            closes = driver.find_element(By.CSS_SELECTOR,
                                         "ul.job-info-metadata > li:nth-child(4) > span:nth-child(2)").text
        except NoSuchElementException:
            closes = "No close time given"

        try:
            all_cards = driver.find_elements(By.CSS_SELECTOR, "div.card-copy")
            description_card = None
            for card in all_cards:
                try:
                    header = card.find_element(By.CSS_SELECTOR, "h2")
                    if "Job description" in header.text:
                        description_card = card
                        break
                except Exception:
                    continue
            if description_card:
                paragraphs = description_card.find_elements(By.TAG_NAME, "p")
                job_description = "\n".join([p.text for p in paragraphs])
            else:
                job_description = "No description given"

        except NoSuchElementException:
            job_description = "No description given"

        try:
            va_map = driver.find_element(By.CSS_SELECTOR, "a[class='custom mint-button secondary direction-btn']")
            link = va_map.get_attribute("href")
            driver.get(link)
            wait_for_page_load(driver)
            map_url = driver.current_url
            pattern = r"@(-?\d+\.\d+),(-?\d+\.\d+)"
            match = re.search(pattern, map_url)
            if match:
                va_lat, va_long = match.groups()
            else:
                va_lat = "No lat given"
                va_long = "No long given"
        except NoSuchElementException:
            va_lat = "No lat given"
            va_long = "No long given"

        va_job_link = f'=HYPERLINK("{vac_element['job_link']}", "{vac_element['job_link']}")'

        va_data = [
            "",
            "",
            vac_element["date_added"],
            vac_element["time_scrapped"],
            vac_element['job_title'],
            va_job_link,
            vac_element['job_code'],
            company,
            salary,
            address,
            va_lat,
            va_long,
            tenure,
            vac_element['overview'],
            closes,
            job_description
        ]
        append_row_with_retry(va_sheet, va_data)
        time.sleep(3)
        seen_jobs.add(vac_element['job_code'])
        vac_index += 1

    progress = {"Phase": "Scrapping", "finished": True, "UrlNum": 1}
    progress_manager.save_progress(progress)

def main():
    driver = set_driver()
    try:
        progress_sheet = get_worksheet("Progress")
        if is_first_execution(progress_sheet):
            set_vacancy_sheet()
        progress_manager = ProgressManager(progress_sheet)
        progress = progress_manager.load_progress()

        while not progress.get("finished"):
            phase = progress.get("Phase")

            if phase == "Scrapping":
                scrapping(driver)

            elif phase == "Detail":
                detail(driver)
            progress = progress_manager.load_progress()

        print("Finished scrapping vacancy process.")

    finally:
        driver.quit()
        print("Saved every data into the Google Sheet successfully.")

if __name__ == "__main__":
    main()
