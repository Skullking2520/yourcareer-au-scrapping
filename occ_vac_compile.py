import json
from selenium import webdriver  # web scrapping
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.oauth2.service_account import Credentials  # google doc
from requests.exceptions import ReadTimeout
import time
import os
import re
import gspread

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

def get_worksheet(sheet_name):
    return spreadsheet.worksheet(sheet_name)

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

def remove_hyperlink(cell_value):
    # remove hyper link
    if cell_value.startswith('=HYPERLINK('):
        pattern = r'=HYPERLINK\("([^"]+)"\s*,\s*"[^"]+"\)'
        match = re.match(pattern, cell_value)
        if match:
            return match.group(1)
    return cell_value

def extract():
    # extract occupation link, title, vacancy link
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

    for row_num, row in enumerate(all_rows, start=2):
        occupation = row[occupation_idx - 1] if len(row) >= occupation_idx else ""
        occupation_link = row[occupation_link_idx - 1] if len(row) >= occupation_link_idx else ""
        vacancies_value = row[vacancies_idx - 1] if len(row) >= vacancies_idx else ""

        vacancies_url = remove_hyperlink(vacancies_value)

        mod_va_occupation = f"{occupation}:{occupation_link}"
        occupation_list.append([mod_va_occupation, vacancies_url])
    return occupation_list

def load_required_vacancy():

    va_sheet = get_worksheet("Vacancies")
    va_header = va_sheet.row_values(1)

    try:
        job_code_index = va_header.index("job code") + 1
        occupation_index = va_header.index("occupation") + 1
    except ValueError:
        return

    rows = va_sheet.get_all_values()
    va_job_code = {}
    for row_num, row in enumerate(rows[1:], start=2):
        job_code = row[job_code_index - 1] if len(row) >= job_code_index else ""
        occ_ori = row[occupation_index - 1] if len(row) >= occupation_index else ""
        va_job_code[job_code] = {"row": row_num, "occupation": occ_ori}
    return va_job_code, occupation_index, va_sheet

def save_cache(job_code, va_occupation, job_code_cache, pending_updates, occupation_index):
    if job_code in job_code_cache:
        row = job_code_cache[job_code]["row"]
        current_value = job_code_cache[job_code]["occupation"]
        if current_value:
            new_value = current_value + "," + str(va_occupation)
        else:
            new_value = str(va_occupation)
        job_code_cache[job_code]["occupation"] = new_value
        pending_updates[(row, occupation_index)] = new_value
    else:
        print(f"job_code:{job_code} not in cache")

def update_occ_row(va_sheet, pending_updates):
    if pending_updates:
        cells_to_update = []
        for (row, col), value in pending_updates.items():
            cells_to_update.append(gspread.Cell(row, col, value))
        try:
            va_sheet.update_cells(cells_to_update, value_input_option="USER_ENTERED")
            pending_updates.clear()
        except Exception:
            print("Cache update error")

_progress_cache_occ = None

def is_first_execution(progress_sheet):
    global _progress_cache_occ
    if _progress_cache_occ is not None:
        return not bool(_progress_cache_occ.strip())
    progress_value = progress_sheet.acell("A3").value
    _progress_cache_occ = progress_value if progress_value else ""
    return not _progress_cache_occ.strip()

def save_progress_occ(progress_sheet, progress):
    global _progress_cache_occ
    if is_first_execution(progress_sheet):
        progress = {"finished": False , "OccIndex": 0}
    try:
        _progress_cache_occ = json.dumps(progress)
        progress_sheet.update("A3", json.dumps(progress))
    except Exception:
        print("Failed to save progress.")

def load_progress_occ(progress_sheet):
    global _progress_cache_occ
    try:
        if _progress_cache_occ is not None:
            return json.loads(_progress_cache_occ)
        if is_first_execution(progress_sheet):
            progress = {"finished": False, "OccIndex": 0}
            _progress_cache_occ = json.dumps(progress)
            return progress
        else:
            progress_json = progress_sheet.acell("A3").value
            _progress_cache_occ = progress_json if progress_json else ""
            if progress_json:
                progress = json.loads(progress_json)
                return progress
            else:
                raise Exception("No progress value found in A3")
    except Exception:
        print("Failed to load progress, finishing program")
        return {"finished": True}

def main():
    driver = set_driver()
    wait = WebDriverWait(driver, 10)
    progress_sheet = get_worksheet("Progress")
    progress = load_progress_occ(progress_sheet)

    extracted_list = extract()
    if not extracted_list:
        print("No Occupation data, shutting down program.")
        driver.quit()
        return

    job_code_cache, occupation_index, va_sheet = load_required_vacancy()
    if not job_code_cache:
        print("Failed to load vacancy cache, shutting down program.")
        driver.quit()
        return

    pending_updates = {}
    check_list = list(job_code_cache.keys())
    occ_index = progress.get("OccIndex", 0)

    while occ_index < len(extracted_list):
        url_data = extracted_list[occ_index]
        va_occupation = url_data[0]
        va_url = url_data[1]

        try:
            driver.get(va_url)
        except Exception:
            print(f"Failed to load page: {va_url}")
            occ_index += 1
            continue

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

        vac_index = 0 # reset VacIndex for this OccIndex
        while True:
            try:
                vacancies = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "section.mint-search-result-item.has-img.has-actions.has-preheading")))
            except TimeoutException:
                print(f"Vacancy elements for occupation index({occ_index}) did not load in time.")
                break

            vac_index = 0
            while vac_index < len(vacancies):
                # find job code, update occupation index
                vacancy = vacancies[vac_index]
                try:
                    job_hyper = vacancy.find_element(By.CSS_SELECTOR, "a[class='mint-link link']")
                    job_href = job_hyper.get_attribute("href")
                    job_code = job_href.split('/')[-1]
                except NoSuchElementException:
                    job_code = "No job code given"

                if job_code in check_list:
                    save_cache(job_code, va_occupation, job_code_cache, pending_updates, occupation_index)
                else:
                    print(f"{job_code} not in list.")

                vac_index += 1

                progress = {"finished": False, "OccIndex": occ_index}
                save_progress_occ(progress_sheet, progress)
            update_occ_row(va_sheet, pending_updates)

            try:
                next_button = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "button[aria-label='Go to next page']")))
                driver.execute_script("arguments[0].click();", next_button)
                time.sleep(3)
            except (NoSuchElementException, TimeoutException):
                break
            except Exception as e:
                print(f"An error occurred while finding next button: {e}")
                break

        occ_index += 1
        progress = {"finished": False, "OccIndex": occ_index}
        save_progress_occ(progress_sheet, progress)

    update_occ_row(va_sheet, pending_updates)
    progress = {"finished": True, "OccIndex": occ_index}
    save_progress_occ(progress_sheet, progress)
    driver.quit()
    print("Saved every data into the Google Sheet successfully.")

if __name__ == "__main__":
    main()
