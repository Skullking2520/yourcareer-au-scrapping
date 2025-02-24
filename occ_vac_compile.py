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
    oc_sheet = web_sheet.get_worksheet("Occupation")
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
        vacancies_url = row[vacancies_idx - 1] if len(row) >= vacancies_idx else ""
        occupation_list.append([occupation, occupation_link, vacancies_url])
    return occupation_list

def extract_vacancy():
    va_sheet = web_sheet.get_worksheet("Vacancies")
    va_header = va_sheet.row_values(1)

    try:
        job_code_index = va_header.index("job code") + 1
        occupation_index = va_header.index("occupation") + 1
    except ValueError:
        return

    rows = va_sheet.get_all_values()
    vacancy_list = []

    for row_num, row in enumerate(rows[1:], start=2):
        job_code = row[job_code_index - 1] if len(row) >= job_code_index else ""
        occ_ori = row[occupation_index - 1] if len(row) >= occupation_index else ""
        vacancy_list.append([job_code, row_num])
    return vacancy_list

def batch_update_cells(worksheet, row_num, updates):
    sheet_id = getattr(worksheet, 'id', None) or worksheet._properties.get('sheetId')
    requests = []

    for col, value in updates:
        requests.append({
            "updateCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row_num - 1,
                    "endRowIndex": row_num,
                    "startColumnIndex": col - 1,
                    "endColumnIndex": col
                },
                "rows": [{
                    "values": [{
                        "userEnteredValue": {"stringValue": str(value)}
                    }]
                }],
                "fields": "userEnteredValue"
            }
        })

    body = {"requests": requests}
    worksheet.spreadsheet.batch_update(body)

def main():
    wait = WebDriverWait(driver, 10)
    va_sheet = web_sheet.get_worksheet("Vacancies")
    progress_sheet = web_sheet.get_worksheet("Progress")
    ph = ProcessHandler(progress_sheet, {"progress": "setting", "RowNum": 0}, "A5")
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

    print("루프 시작: progress =", progress, ", RowNum =", progress["RowNum"])
    while not progress["progress"] == "finished":
        print("루프 내부 시작: progress =", progress, ", RowNum =", progress["RowNum"])
        progress["progress"] = "processing"
        occ_data = occ_extracted_list[progress["RowNum"]]
        occ_name = occ_data[0]
        occ_url = occ_data[1]
        va_url = occ_data[2]
        print(f"[main] Before driver.get - Processing Row {progress['RowNum']+1}: {occ_name}")
        
        try:
            print(f"[main] Calling driver.get({va_url})")
            driver.get(va_url)
            print("[main] After driver.get")
        except Exception:
            print(f"Failed to load page: {occ_name}")
            progress["RowNum"] += 1
            continue

        progress["RowNum"] += 1
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        print("[main] Before wait_for_page_load")
        wait_for_page_load(driver)
        print("[main] After wait_for_page_load - current page:", occ_name)

        match_index = []

        for va in vac_extracted_list:
            try:
                print("[main] Before wait.until for vacancy elements")
                vacancies = wait.until(EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "section.mint-search-result-item.has-img.has-actions.has-preheading")))
                print("[main] After wait.until for vacancy elements")
            except TimeoutException:
                print(f"Vacancy elements did not load in time.")
                break
            except Exception as e:
                print(f"An error occurred while waiting for page load: {e}")
                break

            for vacancy in vacancies:
                try:
                    job_hyper = vacancy.find_element(By.CSS_SELECTOR, "a[class='mint-link link']")
                    job_href = job_hyper.get_attribute("href")
                    job_code = job_href.split('/')[-1]
                except NoSuchElementException:
                    job_code = "No job code given"

                if va[0] == job_code:
                    match_index.append(va[1])

            for row_num in match_index:
                update = [
                    (col_occupation, occ_name),
                    (col_occ_link, occ_url)
                ]
                batch_update_cells(va_sheet, row_num, update)
                time.sleep(3)

        try:
            driver.find_element(By.CSS_SELECTOR, "button[aria-label='Go to next page']")
        except NoSuchElementException:
            progress["progress"] = "finished"
            ph.save_progress(progress)
            print("Finished scrapping")
            break
        except Exception as e:
            print(f"An error occurred while finding next button: {e}")
            break

    progress_sheet.update("A5", [[json.dumps({"progress": "setting", "RowNum": 0})]])
    driver.quit()
    print("Saved every data into the Google Sheet successfully.")

if __name__ == "__main__":
    main()
