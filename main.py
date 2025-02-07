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
spreadsheet_url = "https://docs.google.com/spreadsheets/d/13fIG9eUVVH1OKkQ6CaaTNSr1Cb8eUg-qCNXxm9m7eu0/edit?gid=0#gid=0"
credentials = Credentials.from_service_account_file(key_path, scopes=scopes)
gc = gspread.authorize(credentials)
spreadsheet = gc.open_by_url(spreadsheet_url)

def set_driver():
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
    options = webdriver.ChromeOptions()
    options.add_argument(f"user-agent={user_agent}")
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-extensions")
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=options)
    return driver

def get_worksheet(sheet_name):
    return spreadsheet.worksheet(sheet_name)

def set_vacancy_sheet():
    worksheet = get_worksheet("Vacancies")
    worksheet.clear()
    headers = ["occupation", "occupation link", "date added", "time scrapped", "job title", "job link", "job code", "company", "salary", "address", "lat", "long", "tenure", "overview", "closes", "description"]
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
                raise

def extract():
    oc_sheet = get_worksheet("Occupation")
    oc_header = oc_sheet.row_values(1)
    try:
        occupation_idx = oc_header.index("occupation") + 1
        occupation_link_idx = oc_header.index("occupation link") + 1
        vacancies_idx = oc_header.index("link to vacancies") + 1
    except ValueError as e:
        print("Could not detect requested row", e)
        return
    all_rows = oc_sheet.get_all_values()[1:]
    occupation_list = []
    for row in all_rows:
        occupation = row[occupation_idx - 1] if len(row) >= occupation_idx else ""
        raw_occ_link = row[occupation_link_idx - 1] if len(row) >= occupation_link_idx else ""
        vacancies_value = row[vacancies_idx - 1] if len(row) >= vacancies_idx else ""
        vacancies_url = remove_hyperlink(vacancies_value)
        if vacancies_url:
            vacancies_url = vacancies_url + "&jobAge=3"
        else:
            vacancies_url = ""
        mod_va_occupation = f"{occupation}:{raw_occ_link}"
        occupation_list.append([occupation, mod_va_occupation, vacancies_url])
    return occupation_list

def check_extract():
    va_sheet = get_worksheet("Vacancies")
    va_header = va_sheet.row_values(1)
    try:
        va_code = va_header.index("job code") + 1
    except ValueError as e:
        print("Could not detect requested row", e)
        return
    all_rows = va_sheet.get_all_values()[1:]
    check_list = []
    for row_num, row in enumerate(all_rows, start=2):
        code = row[va_code - 1] if len(row) >= va_code else ""
        check_list.append(code)
    return check_list

def update_occupation_cell(job_code, new_occupation, new_mod_va_occupation, retries=3, delay=5):
    va_sheet = get_worksheet("Vacancies")
    va_header = va_sheet.row_values(1)
    try:
        job_code_index = va_header.index("job code") + 1
        occupation_index = va_header.index("occupation") + 1
        occ_link_index = va_header.index("occupation link") + 1
    except ValueError:
        return
    all_rows = va_sheet.get_all_values()[1:]
    for row_num, row in enumerate(all_rows, start=2):
        code = row[job_code_index - 1] if len(row) >= job_code_index else ""
        if job_code == code:
            for attempt in range(retries):
                try:
                    current_occ = va_sheet.cell(row_num, occupation_index).value
                    if current_occ and new_occupation in current_occ.split(","):
                        break
                    updated_occ = f"{current_occ},{new_occupation}" if current_occ else new_occupation
                    va_sheet.update_cell(row_num, occupation_index, updated_occ)
                    current_occ_link = va_sheet.cell(row_num, occ_link_index).value
                    updated_occ_link = f"{current_occ_link},{new_mod_va_occupation}" if current_occ_link else new_mod_va_occupation
                    va_sheet.update_cell(row_num, occ_link_index, updated_occ_link)
                    break
                except Exception:
                    time.sleep(delay)
            break

def remove_hyperlink(cell_value):
    if cell_value.startswith('=HYPERLINK('):
        pattern = r'=HYPERLINK\("([^"]+)"\s*,\s*"[^"]+"\)'
        match = re.match(pattern, cell_value)
        if match:
            return match.group(1)
    return cell_value

def set_progress_sheet_vac():
    try:
        progress_sheet = spreadsheet.worksheet("VacancyData")
    except gspread.exceptions.WorksheetNotFound:
        progress_sheet = spreadsheet.add_worksheet("VacancyData", rows="100", cols="10")
        progress_sheet.clear()
    return progress_sheet

def load_progress_vac(progress_sheet):
    try:
        progress_json = progress_sheet.acell("A2").value
        if progress_json:
            progress = json.loads(progress_json)
            return progress
        else:
            raise Exception("No progress value found in A2")
    except Exception:
        return {"outer": 0, "phase": "vacancy_extraction", "vacancy_index": 0, "detail_index": 0}

def save_progress_vac(progress_sheet, progress):
    progress_sheet.update(values=[[json.dumps(progress)]], range_name="A2")

def main():
    set_vacancy_sheet()
    va_sheet = get_worksheet("Vacancies")
    progress_sheet = set_progress_sheet_vac()
    driver = set_driver()
    progress = load_progress_vac(progress_sheet)
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
        
        all_vacancy_data = []
        seen_jobs = set()
        check_list = check_extract()
        
        driver.get(va_url)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        
        if phase == "vacancy_extraction":
            while True:
                vacancies = driver.find_elements(By.CSS_SELECTOR, "section[class='mint-search-result-item has-img has-actions has-preheading']")
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
                    all_vacancy_data.append(vacancy_data)
                    
                    vac_idx += 1
                    progress["vacancy_index"] = vac_idx
                    save_progress_vac(progress_sheet, progress)
                
                vacancy_index = 0
                progress["vacancy_index"] = 0
                save_progress_vac(progress_sheet, progress)
                
                try:
                    next_button = driver.find_element(By.CSS_SELECTOR, "button[aria-label='Go to next page']")
                    driver.execute_script("arguments[0].click();", next_button)
                    time.sleep(3)
                except NoSuchElementException:
                    break
            
            progress["phase"] = "detail_extraction"
            progress["detail_index"] = 0
            save_progress_vac(progress_sheet, progress)
        else:
            pass
        
        if progress.get("phase") == "detail_extraction":
            detail_idx = progress.get("detail_index", 0)
            while detail_idx < len(all_vacancy_data):
                vac_element = all_vacancy_data[detail_idx]
                driver.get(vac_element["job_link"])
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)
                print(f"current page: {vac_element['job_title']}")
                try:
                    company_elem = WebDriverWait(driver, 10).until(
                        EC.visibility_of_element_located((By.XPATH, "//*[@id='find-a-job']//div[contains(@class, 'text-lg')]//p/a"))
                    )
                    company = company_elem.text
                except (NoSuchElementException, TimeoutException, TimeoutError):
                    company = "No company given"
                
                if vac_element['job_code'] in seen_jobs:
                    print(f"Duplicate found, skipping: {company}, {vac_element['job_title']}")
                    detail_idx += 1
                    progress["detail_index"] = detail_idx
                    save_progress_vac(progress_sheet, progress)
                    continue
                
                try:
                    address = driver.find_element(By.CSS_SELECTOR, "div[class='address-text']").text
                except NoSuchElementException:
                    address = "No address given"
                
                try:
                    salary = driver.find_element(By.CSS_SELECTOR, "ul.job-info-metadata > li:nth-child(2) > span:nth-of-type(2)").text
                except NoSuchElementException:
                    salary = "No salary given"
                
                try:
                    tenure = driver.find_element(By.CSS_SELECTOR, "ul.job-info-metadata > li:nth-child(3) > span:nth-of-type(2)").text
                except NoSuchElementException:
                    tenure = "No tenure given"
                
                try:
                    closes = driver.find_element(By.CSS_SELECTOR, "ul.job-info-metadata > li:nth-child(4) > span:nth-child(2)").text
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
                
                if vac_element['job_code'] in check_extract():
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
                
                detail_idx += 1
                progress["detail_index"] = detail_idx
                save_progress_vac(progress_sheet, progress)
        
        outer += 1
        progress = {"outer": outer, "phase": "vacancy_extraction", "vacancy_index": 0, "detail_index": 0}
        save_progress_vac(progress_sheet, progress)
    
    driver.quit()
    print("Saved every data into the Google Sheet successfully.")

if __name__ == "__main__":
    main()
