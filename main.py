from urllib.parse import urljoin
from selenium import webdriver  # web scrapping
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.oauth2.service_account import Credentials  # google doc
from gspread_formatting import *
import time
import re
import gspread
import os
import json

key_content = os.environ.get("SERVICE_ACCOUNT_KEY")
if not key_content:
    raise FileNotFoundError("Service account key content not found in environment variable!")
key_path = "service_account.json"
with open(key_path, "w") as f:
    f.write(key_content)
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
spreadsheet_url = ("https://docs.google.com/spreadsheets/d/13fIG9eUVVH1OKkQ6CaaTNSr1Cb8eUg-"
                     "qCNXxm9m7eu0/edit?gid=0#gid=0")
credentials = Credentials.from_service_account_file(key_path, scopes=scopes)
gc = gspread.authorize(credentials)
spreadsheet = gc.open_by_url(spreadsheet_url)

def get_worksheet(sheet_name):
    return spreadsheet.worksheet(sheet_name)

def set_vacancy_sheet():
    worksheet = get_worksheet("Vacancies")
    worksheet.clear()
    headers = ["occupation", "occupation link", "date added", "time scrapped", "job title",
               "job link", "job code", "company", "salary", "address", "lat", "long",
               "tenure", "overview", "closes", "description"]
    worksheet.append_row(headers)
    return worksheet

def load_progress_occ():
    progress_sheet = get_worksheet("Progress")
    progress_val = progress_sheet.acell("A2").value
    if not progress_val:
        return {
            "phase": "list",
            "page_num": 1,
            "detail_index": 0,
            "all_occupation_data": [],
            "finished": False
        }
    try:
        progress = json.loads(progress_val)
        minimal_progress = {
            "phase": progress.get("phase", "list"),
            "page_num": progress.get("page_num", 1),
            "detail_index": progress.get("detail_index", 0),
            "all_occupation_data": progress.get("all_occupation_data", []),
            "finished": progress.get("finished", False)
        }
        return minimal_progress
    except Exception as e:
        print("Error loading occupation progress from sheet, using default:", e)
        return {
            "phase": "list",
            "page_num": 1,
            "detail_index": 0,
            "all_occupation_data": [],
            "finished": False
        }

def save_progress_occ(progress):
    progress_sheet = get_worksheet("Progress")
    minimal_progress = {
        "phase": progress.get("phase", "list"),
        "page_num": progress.get("page_num", 1),
        "detail_index": progress.get("detail_index", 0),
        "all_occupation_data": progress.get("all_occupation_data", []),
        "finished": progress.get("finished", False)
    }
    progress_sheet.update_acell("A2", json.dumps(minimal_progress))

def set_driver():
    user_agent = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36")
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
            if any(code in str(e) for code in ["500", "502", "503", "504"]):
                print(f"Error 503 occurred. Retry after {delay} seconds ({attempt+1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                raise

def set_sheet():
    sheet_name = "Occupation"
    worksheet = spreadsheet.worksheet(sheet_name)
    worksheet.clear()
    headers = ["occupation code", "occupation", "occupation link", "description", "average salary", "future demand", "job type",
               "skill level", "industry", "skills", "number of vacancies",
               "link to vacancies", "link to courses", "apprenticeships and traineeships",
               "overview : interests", "overview : considerations", "overview : day-to-day"]
    worksheet.append_row(headers)
    header_format = CellFormat(backgroundColor=Color(0.8, 1, 0.8),
                               textFormat=TextFormat(bold=True, fontSize=12),
                               horizontalAlignment='CENTER')
    format_cell_range(worksheet, 'A1:Q1', header_format)
    for col in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'I', 'K', 'L', 'M', 'H']:
        set_column_width(worksheet, col, 150)
    for col in ['J', 'N', 'O', 'Q']:
        set_column_width(worksheet, col, 200)
    for col in ['P']:
        set_column_width(worksheet, col, 300)
    return worksheet

def check_next_button(driver):
    next_button = driver.find_elements(By.CSS_SELECTOR, "button[aria-label='Go to next page']")
    return bool(next_button)

def find_occupation_code(link):
    code = r"/occupations/(\d+)/"
    found = re.findall(code, link)
    return found

def overview_to_skills(link):
    modified_link = re.sub(r"(\?|&)tab=overview", r"\1tab=skills", link)
    return modified_link

def load_occupation_data():
    try:
        ws = get_worksheet("OccupationData")
        all_values = ws.get_all_values()
        if len(all_values) <= 1:
            return []
        data = []
        for row in all_values[1:]:
            record = {
                "detail_url": row[0],
                "occupation_name": row[1],
                "num_vacancy": row[2],
                "vacancy_hyper_link": row[3],
                "courses_url_escaped": row[4]
            }
            data.append(record)
        return data
    except Exception as e:
        print("Error loading occupation data from sheet:", e)
        return []

def save_occupation_data(data):
    try:
        ws = get_worksheet("OccupationData")
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet("OccupationData", rows="1000", cols="20")
    headers = ["detail_url", "occupation_name", "num_vacancy", "vacancy_hyper_link", "courses_url_escaped"]
    ws.append_row(headers)
    for record in data:
        ws.append_row([
            record.get("detail_url", ""),
            record.get("occupation_name", ""),
            record.get("num_vacancy", ""),
            record.get("vacancy_hyper_link", ""),
            record.get("courses_url_escaped", "")
        ])

def main():
    init = set_vacancy_sheet()
    va_sheet = get_worksheet("Vacancies")
    driver = set_driver()
    progress = load_progress_occ()
    url_data_list = list(extract())
    outer = progress.get("outer", 0)
    all_occupation_data = []
    page_num = progress.get("page_num", 1)
    
    while outer < len(url_data_list):
        phase = progress.get("phase", "list")
        vacancy_index = progress.get("vacancy_index", 0)
        detail_idx = progress.get("detail_index", 0)
        url_data = url_data_list[outer]
        occupation = url_data[0]
        va_occupation = url_data[1]
        va_url = url_data[2]
        all_occupation_data = []
        seen_jobs = set()
        check_list = check_extract()
        driver.get(va_url)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        if phase == "list":
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
                    vacancy_data = {
                        "job_title": job_title,
                        "job_link": job_link,
                        "job_code": job_code,
                        "date_added": str(date_added),
                        "time_scrapped": str(time_scrapped),
                        "overview": overview
                    }
                    all_occupation_data.append(vacancy_data)
                    vac_idx += 1
                    progress["vacancy_index"] = vac_idx
                    save_progress_occ(progress)
                vacancy_index = 0
                progress["vacancy_index"] = 0
                save_progress_occ(progress)
                try:
                    next_button = driver.find_element(By.CSS_SELECTOR, "button[aria-label='Go to next page']")
                    driver.execute_script("arguments[0].click();", next_button)
                    time.sleep(3)
                except NoSuchElementException:
                    break
                page_num += 1
                progress["page_num"] = page_num
                save_progress_occ(progress)
            save_occupation_data(all_occupation_data)
            progress["phase"] = "detail_extraction"
            progress["detail_index"] = 0
            save_progress_occ(progress)
        else:
            all_occupation_data = load_occupation_data()
        detail_index = progress.get("detail_index", 0)
        check_list = check_extract()
        seen_jobs = set()
        for i in range(detail_index, len(all_occupation_data)):
            vac_element = all_occupation_data[i]
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
            if vac_element['job_code'] in seen_jobs:
                print(f"Duplicate found, skipping: {company}, {vac_element['job_title']}")
                detail_index += 1
                progress["detail_index"] = detail_index
                continue
            try:
                address = driver.find_element(By.CSS_SELECTOR, "div[class='address-text']").text
            except NoSuchElementException:
                address = "No address given"
            try:
                salary = driver.find_element(By.CSS_SELECTOR,
                    "ul.job-info-metadata > li:nth-child(2) > span:nth-of-type(2)").text
            except NoSuchElementException:
                salary = "No salary given"
            try:
                tenure = driver.find_element(By.CSS_SELECTOR,
                    "ul.job-info-metadata > li:nth-child(3) > span:nth-of-type(2)").text
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
                time.sleep(10)
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
            va_job_link = f'=HYPERLINK("{vac_element["job_link"]}", "{vac_element["job_link"]}")'
            if vac_element['job_code'] in check_list:
                update_occupation_cell(vac_element['job_code'], occupation, va_occupation)
            else:
                va_data = [
                    occupation,
                    va_occupation,
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
                seen_jobs.add(vac_element['job_code'])
            detail_index += 1
            progress["detail_index"] = detail_index
            save_progress_occ(progress)
        outer += 1
        progress["outer"] = outer
        progress["vacancy_index"] = 0
        progress["detail_index"] = 0
        save_progress_occ(progress)
    progress["finished"] = True
    save_progress_occ(progress)
    driver.quit()
    print("Saved every data into the Google Sheet successfully.")

if __name__ == "__main__":
    main()
