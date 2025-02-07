from urllib.parse import urljoin
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.oauth2.service_account import Credentials
from requests.exceptions import ReadTimeout
import time
import os
import re
import gspread
import datetime
import json

# Set up service account credentials
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
spreadsheet_url = ("https://docs.google.com/spreadsheets/d/13fIG9eUVVH1OKkQ6CaaTNSr1Cb8eUg-"
                     "qCNXxm9m7eu0/edit?gid=0#gid=0")
credentials = Credentials.from_service_account_file(key_path, scopes=scopes)
gc = gspread.authorize(credentials)
sh = gc.open_by_url(spreadsheet_url)

def load_progress_occ(progress_sheet):
    try:
        progress_json = progress_sheet.acell("A2").value
        if progress_json:
            progress = json.loads(progress_json)
            progress.pop("all_occupation_data", None)
            return progress
        else:
            raise Exception("No progress value found in A2")
    except Exception:
        return {
            "phase": "vacancy_extraction",
            "vacancy_index": 0,
            "detail_index": 0,
            "outer": 0,
            "finished": False
        }

def save_progress_occ(progress_sheet, progress):
    progress_sheet.update(values=[[json.dumps(progress)]], range_name="A2")

def set_vac_data_sheet():
    vac_data_sheet = sh.worksheet("VacancyData")
    vac_data_sheet.clear()
    headers = ["occupation", "occupation link", "date added", "time scrapped",
               "job title", "job link", "job code", "company", "salary", "address",
               "lat", "long", "tenure", "overview", "closes", "description"]
    vac_data_sheet.append_row(headers)

def set_vacancy_sheet():
    vac_sheet = sh.worksheet("Vacancies")
    vac_sheet.clear()
    headers = ["occupation", "occupation link", "date added", "time scrapped",
               "job title", "job link", "job code", "company", "salary", "address",
               "lat", "long", "tenure", "overview", "closes", "description"]
    vac_sheet.append_row(headers)

def set_sheets():
    try:
        vacancy_sheet = sh.worksheet("Vacancies")
    except gspread.exceptions.WorksheetNotFound:
        vacancy_sheet = sh.add_worksheet("Vacancies", rows="1000", cols="20")
    try:
        progress_sheet = sh.worksheet("Progress")
    except gspread.exceptions.WorksheetNotFound:
        progress_sheet = sh.add_worksheet("Progress", rows="100", cols="10")
    try:
        vac_data_sheet = sh.worksheet("VacancyData")
    except gspread.exceptions.WorksheetNotFound:
        vac_data_sheet = sh.add_worksheet("VacancyData", rows="1000", cols="20")
    return vacancy_sheet, progress_sheet, vac_data_sheet

def set_driver():
    user_agent = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/132.0.0.0 Safari/537.36")
    options = webdriver.ChromeOptions()
    options.add_argument(f"user-agent={user_agent}")
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-extensions")
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=options)
    return driver

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
                raise

def check_next_button(driver):
    next_button = driver.find_elements(By.CSS_SELECTOR, "button[aria-label='Go to next page']")
    return bool(next_button)

def find_occupation_code(link):
    code = r"/occupations/(\d+)/"
    return re.findall(code, link)

def overview_to_skills(link):
    return re.sub(r"(\?|&)tab=overview", r"\1tab=skills", link)

def remove_hyperlink(cell_value):
    if cell_value.startswith('=HYPERLINK('):
        pattern = r'=HYPERLINK\("([^"]+)"\s*,\s*"[^"]+"\)'
        match = re.match(pattern, cell_value)
        if match:
            return match.group(1)
    return cell_value

def is_first_execution(progress_sheet):
    val = progress_sheet.acell("A2").value
    return not val or val.strip() == ""

def main():
    vacancy_sheet, progress_sheet, vac_data_sheet = set_sheets()
    if is_first_execution(progress_sheet):
        set_vacancy_sheet()
    set_vac_data_sheet()
    
    progress = load_progress_occ(progress_sheet)
    if progress.get("finished", False):
        print("Process already finished.")
        return

    driver = set_driver()
    url_data_list = list(extract())
    outer = progress.get("outer", 0)
    
    while outer < len(url_data_list):
        phase = progress.get("phase", "vacancy_extraction")
        vacancy_index = progress.get("vacancy_index", 0)
        detail_idx = progress.get("detail_index", 0)
        
        url_data = url_data_list[outer]
        occupation = url_data[0]
        va_occupation = url_data[1]
        va_url = url_data[2]
        
        rows_to_append = []
        
        driver.get(va_url)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        
        if phase == "vacancy_extraction":
            while True:
                vacancies = driver.find_elements(By.CSS_SELECTOR, 
                    "section[class='mint-search-result-item has-img has-actions has-preheading']")
                time.sleep(3)
                vac_idx = vacancy_index
                while vac_idx < len(vacancies):
                    vacancy = vacancies[vac_idx]
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
                    
                    row = [occupation, va_occupation, str(date_added), str(time_scrapped),
                           job_title, job_link, job_code, "", "", "", "", "", "", overview, "", ""]
                    rows_to_append.append(row)
                    
                    vac_idx += 1
                    progress["vacancy_index"] = vac_idx
                    save_progress_occ(progress_sheet, progress)
                vacancy_index = 0
                progress["vacancy_index"] = 0
                save_progress_occ(progress_sheet, progress)
                try:
                    next_button = driver.find_element(By.CSS_SELECTOR, "button[aria-label='Go to next page']")
                    driver.execute_script("arguments[0].click();", next_button)
                    time.sleep(3)
                except NoSuchElementException:
                    break
            if rows_to_append:
                try:
                    vac_data_sheet.append_rows(rows_to_append, value_input_option="USER_ENTERED")
                except Exception as e:
                    print("Error appending rows:", e)
            progress["phase"] = "detail_extraction"
            progress["detail_index"] = 0
            save_progress_occ(progress_sheet, progress)
        else:
            records = vac_data_sheet.get_all_records()
            all_vacancy_data = []
            for record in records:
                all_vacancy_data.append({
                    "job_title": record["job title"],
                    "job_link": record["job link"],
                    "job_code": record["job code"],
                    "date_added": record["date added"],
                    "time_scrapped": record["time scrapped"],
                    "overview": record["overview"]
                })
            if 'all_vacancy_data' not in locals():
                all_vacancy_data = []
                for record in records:
                    all_vacancy_data.append({
                        "job_title": record["job title"],
                        "job_link": record["job link"],
                        "job_code": record["job code"],
                        "date_added": record["date added"],
                        "time scrapped": record["time scrapped"],
                        "overview": record["overview"]
                    })
        detail_idx = progress.get("detail_index", 0)
        for i in range(detail_idx, len(all_vacancy_data)):
            vac_element = all_vacancy_data[i]
            driver.get(vac_element["job_link"])
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            print(f"current page: {vac_element['job_title']}")
            try:
                company_elem = WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located(
                        (By.XPATH, "//*[@id='find-a-job']//div[contains(@class, 'text-lg')]//p/a")
                    )
                )
                company = company_elem.text
            except (NoSuchElementException, TimeoutException, TimeoutError):
                company = "No company given"
            if vac_element['job_code'] in check_extract():
                update_occupation_cell(vac_element['job_code'], occupation, va_occupation)
            else:
                va_data = [
                    occupation,
                    va_occupation,
                    vac_element["date_added"],
                    vac_element["time_scrapped"],
                    vac_element['job_title'],
                    f'=HYPERLINK("{vac_element["job_link"]}", "{vac_element["job_link"]}")',
                    vac_element['job_code'],
                    company,
                    "No salary given",
                    "No address given",
                    "No lat given",
                    "No long given",
                    "No tenure given",
                    vac_element['overview'],
                    "No close time given",
                    "No description given"
                ]
                append_row_with_retry(vac_data_sheet, va_data)
            detail_idx += 1
            progress["detail_index"] = detail_idx
            save_progress_occ(progress_sheet, progress)
        outer += 1
        progress = {"outer": outer, "phase": "vacancy_extraction", "vacancy_index": 0, "detail_index": 0}
        save_progress_occ(progress_sheet, progress)
    driver.quit()
    print("Saved every data into the Google Sheet successfully.")

if __name__ == "__main__":
    main()
