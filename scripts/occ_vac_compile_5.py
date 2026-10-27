# occ_vac_compile.py
import json
import time

import gspread
from requests.exceptions import ReadTimeout
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from google_form_package import Sheet
from process_handler import ProcessHandler

web_sheet = Sheet()
driver = web_sheet.set_driver()

def get_worksheet_with_retry(sheet_name, retries=3, delay=5):
    for attempt in range(retries):
        try:
            ws = web_sheet.get_worksheet(sheet_name)
            return ws
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                print(f"Read quota error for {sheet_name}. Retrying in {delay} seconds... (Attempt {attempt+1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    raise Exception(f"Failed to get worksheet {sheet_name} after {retries} attempts.")

def append_row_with_retry(worksheet, data, retries=3, delay=5):
    for attempt in range(retries):
        try:
            worksheet.append_row(data, value_input_option="USER_ENTERED")
            return
        except gspread.exceptions.APIError as e:
            if any(code in str(e) for code in ["500", "502", "503", "504"]) or isinstance(e, ReadTimeout):
                print(f"Error occurred. Retry after {delay} seconds ({attempt + 1}/{retries})")
                time.sleep(delay)
            else:
                raise

def wait_for_page_load(wait_driver, timeout=15):
    try:
        WebDriverWait(wait_driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException:
        print("Page loading timeout.")
    except Exception as e:
        print(f"An error occurred while waiting for page load: {e}")

def extract_occupation():
    # extract occupation link, title, vacancy link
    oc_sheet = get_worksheet_with_retry("Occupation")
    delay = 5
    for attempt in range(3):
        try:
            oc_header = oc_sheet.row_values(1)
            break
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                print(f"Read quota error when fetching row 1. Retrying in {delay} seconds... (Attempt {attempt+1}/3)")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    else:
        raise Exception("Failed to fetch header after 3 attempts.")
    try:
        occupation_idx = oc_header.index("occupation") + 1
        occupation_link_idx = oc_header.index("occupation link") + 1
        vacancies_idx = oc_header.index("link to vacancies") + 1
    except ValueError as e:
        print("Could not detect requested row", e)
        return

    for attempt in range(3):
        try:
            all_rows = oc_sheet.get_all_values()[1:]
            break
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                print(f"Read quota error when fetching all values. Retrying in {delay} seconds... (Attempt {attempt+1}/3)")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    else:
        raise Exception("Failed to fetch all values after 3 attempts.")

    occupation_list = []

    for row_num, row in enumerate(all_rows, start=2):
        occupation = row[occupation_idx - 1] if len(row) >= occupation_idx else ""
        occupation_link = row[occupation_link_idx - 1] if len(row) >= occupation_link_idx else ""
        vacancies_url = row[vacancies_idx - 1] if len(row) >= vacancies_idx else ""
        occupation_list.append([occupation, occupation_link, vacancies_url])
    return occupation_list

def extract_vacancy():
    va_sheet = web_sheet.get_worksheet("Vacancies")
    delay = 5
    for attempt in range(3):
        try:
            va_header = va_sheet.row_values(1)
            break
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                print(f"Read quota error when fetching row 1 from Vacancies. Retrying in {delay} seconds... (Attempt {attempt+1}/3)")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    else:
        raise Exception("Failed to fetch vacancy header after 3 attempts.")

    try:
        job_code_index = va_header.index("job code") + 1
    except ValueError:
        return

    for attempt in range(3):
        try:
            rows = va_sheet.get_all_values()
            break
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                print(f"Read quota error when fetching all values from Vacancies. Retrying in {delay} seconds... (Attempt {attempt+1}/3)")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    else:
        raise Exception("Failed to fetch vacancy data after 3 attempts.")
    vacancy_list = []

    for row_num, row in enumerate(rows[1:], start=2):
        job_code = row[job_code_index - 1] if len(row) >= job_code_index else ""
        vacancy_list.append([job_code, row_num])
    return vacancy_list

def update_cells_append_batch(worksheet, row_indices, col, new_value, batch_size=100):
    if not row_indices:
        return

    min_row, max_row = min(row_indices), max(row_indices)

    def get_range_with_retry():
        delay = 10
        for attempt in range(3):
            try:
                return worksheet.range(min_row, col, max_row, col)
            except gspread.exceptions.APIError as e:
                if "429" in str(e):
                    print(f"Read quota error when fetching range. Retrying in {delay} seconds... (Attempt {attempt+1}/3)")
                    time.sleep(delay)
                    delay *= 2
                else:
                    raise
        raise Exception("Failed to fetch range after several attempts.")

    cell_range = get_range_with_retry()

    updates = []
    for cell in cell_range:
        if cell.row in row_indices:
            current_value = cell.value or ""
            existing = set(item.strip() for item in current_value.split(",")) if current_value else set()
            if new_value not in existing:
                cell.value = current_value + ("," if current_value else "") + new_value
                updates.append(cell)

    if updates:
        delay = 30
        for attempt in range(3):
            try:
                worksheet.update_cells(updates)
                break
            except gspread.exceptions.APIError as e:
                if "429" in str(e):
                    print(f"Write quota error when updating cells. Retrying in {delay} seconds... (Attempt {attempt+1}/3)")
                    time.sleep(delay)
                    delay *= 2
                else:
                    raise
        else:
            print("Failed to update cells after several attempts.")
def main():
    wait = WebDriverWait(driver, 10)
    va_sheet = get_worksheet_with_retry("Vacancies")
    progress_sheet = get_worksheet_with_retry("Progress")
    ph = ProcessHandler(progress_sheet, {"progress": "setting", "RowNum": 4}, "E5")
    progress = ph.load_progress()
    occ_extracted_list = extract_occupation()
    vac_extracted_list = extract_vacancy()
    vac_sheet_header = va_sheet.row_values(1)
    try:
        col_occupation = vac_sheet_header.index("occupation") + 1
        col_occ_link = vac_sheet_header.index("occupation link") + 1
    except ValueError:
        print("Column not in sheet")
        return

    if not occ_extracted_list:
        print("No Occupation data, shutting down program.")
        driver.quit()
        return

    while progress["RowNum"] < len(occ_extracted_list):
        progress["progress"] = "processing"
        occ_data = occ_extracted_list[progress["RowNum"]]
        occ_name = occ_data[0]
        occ_url = occ_data[1]
        raw_va_url = occ_data[2]
        va_url = str(raw_va_url) + "&pageNumber="

        pagenum = 1
        match_index = []
        prev_job_codes = None
        while True:
            try:
                driver.get(va_url + str(pagenum))
            except Exception:
                print(f"Failed to load page {pagenum}, skipping...")
                progress["RowNum"] += 20
                break

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            wait_for_page_load(driver)

            for attempt in range(3):
                try:
                    vacancies = wait.until(EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, "section.mint-search-result-item.has-img.has-actions.has-preheading")))
                    break
                except TimeoutException:
                    print(f"Vacancy elements did not load in time. Attempt {attempt + 1}")
                except Exception as e:
                    print(f"An error occurred while waiting for page load: {e}. Attempt {attempt + 1}")
            else:
                progress["RowNum"] += 20
                print(f"Vacancy elements did not load in time. Skipping row {progress["RowNum"]}")
                break

            vacancy_dict = {str(va[0]): va[1] for va in vac_extracted_list}
            current_job_codes = []
            for vacancy in vacancies:
                try:
                    job_hyper = vacancy.find_element(By.CSS_SELECTOR, "a[class='mint-link link']")
                    job_href = job_hyper.get_attribute("href")
                    job_code = job_href.split('/')[-1]
                except NoSuchElementException:
                    job_code = "NA"
                current_job_codes.append(job_code)
                if job_code in vacancy_dict:
                    match_index.append(vacancy_dict[job_code])

            if prev_job_codes is not None and set(current_job_codes) == set(prev_job_codes):
                print("Current page vacancy list is identical to the previous page. Ending loop.")
                update_cells_append_batch(va_sheet, match_index, col_occupation, occ_name)
                update_cells_append_batch(va_sheet, match_index, col_occ_link, occ_url)
                time.sleep(3)
                print(f"{occ_name} matching finished, proceeding to next occupation")
                progress["RowNum"] += 20
                match_index = []
                break
        
            prev_job_codes = current_job_codes
            pagenum += 1
            
        progress["progress"] = "finished"
        ph.save_progress(progress)

    driver.quit()
    print("Saved every data into the Google Sheet successfully.")

if __name__ == "__main__":
    main()
