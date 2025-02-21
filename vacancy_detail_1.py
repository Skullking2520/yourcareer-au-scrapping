# vacancy_detail.py
import json
import re
import time
import gspread

from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from google_form_package import Sheet
from process_handler import ProcessHandler

web_sheet = Sheet()
driver = web_sheet.set_driver()

def wait_for_page_load(wait_driver, timeout=15):
    try:
        WebDriverWait(wait_driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException:
        print("Page loading timeout.")
    except Exception as e:
        print(f"An error occurred while waiting for page load: {e}")
        
def extract():
    # extract from Sheet1
    occ_sheet = web_sheet.get_worksheet("Vacancies")
    occ_sheet_header = occ_sheet.row_values(1)
    try:
        link_idx = occ_sheet_header.index("job link") + 1
    except ValueError as e:
        print("Could not detect requested row", e)
        return

    all_rows = occ_sheet.get_all_values()[1:]
    link_list = []

    for row_num, row in enumerate(all_rows, start=2):
        link = row[link_idx - 1] if len(row) >= link_idx else ""
        if not link:
            break
        link_list.append({"link_row_num":row_num, "detail_url":link})
    return link_list

def batch_update_cells(worksheet, row_num, updates, retries=3, delay=10):
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

    for attempt in range(retries):
        try:
            worksheet.spreadsheet.batch_update(body)
            return
        except gspread.exceptions.APIError as e:
            error_message = str(e)
            if "429" in error_message or "503" in error_message:
                print(f"API Error ({error_message}). Retrying after {delay} seconds... (Attempt {attempt+1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    print("Failed to update cells after several attempts.")


def main():
    va_sheet = web_sheet.get_worksheet("Vacancies")
    progress_sheet = web_sheet.get_worksheet("Progress")
    extracted_list = extract()
    ph = ProcessHandler(progress_sheet, {"progress": "setting", "RowNum": 0}, "A4")
    progress = ph.load_progress()
    vac_sheet_header = va_sheet.row_values(1)
    try:
        col_company = vac_sheet_header.index("company") + 1
        col_salary = vac_sheet_header.index("salary") + 1
        col_address = vac_sheet_header.index("address") + 1
        col_lat = vac_sheet_header.index("lat") + 1
        col_long = vac_sheet_header.index("long") + 1
        col_tenure = vac_sheet_header.index("tenure") + 1
        col_closes = vac_sheet_header.index("closes") + 1
        col_description = vac_sheet_header.index("description") + 1

    except ValueError:
        print("Column not in sheet")
        return
    driver.set_page_load_timeout(120)
    while not progress["progress"] == "finished":
        try:
            progress["progress"] = "processing"
            while progress["RowNum"] < len(extracted_list):
                row_and_index = extracted_list[progress["RowNum"]]
                row_num = row_and_index["link_row_num"]
                url = row_and_index["detail_url"]
                if url == "No detail url given":
                    company = "Failed to load detail page"
                    salary = "Failed to load detail page"
                    address = "Failed to load detail page"
                    va_lat = "Failed to load detail page"
                    va_long = "Failed to load detail page"
                    tenure = "Failed to load detail page"
                    closes = "Failed to load detail page"
                    job_description = "Failed to load detail page"
                    print(f"Failed to find detail of row {progress["RowNum"]}. Skipping...")
                    progress["RowNum"] += 2
                else:
                    max_retries = 3
                    loaded = False
                    for attempt in range(1, max_retries + 1):
                        try:
                            print(f"loading page, attempt {attempt}: {url}")
                            driver.get(url)
                            loaded = True
                            break
                        except TimeoutException:
                            print(f"Timeout occured in {attempt} attempt: {url}")
                            if attempt < max_retries:
                                time.sleep(5)
                            else:
                                print("Exceed max retry, skiping page.")
                                loaded = False
                    if not loaded:
                        company = "Failed to load detail page"
                        salary = "Failed to load detail page"
                        address = "Failed to load detail page"
                        va_lat = "Failed to load detail page"
                        va_long = "Failed to load detail page"
                        tenure = "Failed to load detail page"
                        closes = "Failed to load detail page"
                        job_description = "Failed to load detail page"
                        progress["RowNum"] += 2
                        continue
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    wait_for_page_load(driver)
                    print(f"current page: {url}")

                    try:
                        company_element = driver.find_element(By.XPATH, "//p[b[contains(text(), 'Company:')]]")
                        company_text = company_element.text
                        company = company_text.replace("Company:", "").strip()
                    except NoSuchElementException:
                        try:
                            company_elem = WebDriverWait(driver, 10).until(
                                EC.visibility_of_element_located((By.XPATH, "//*[@id='find-a-job']//div[contains(@class, 'text-lg')]//p/a"))
                            )
                            company = company_elem.text.strip()
                        except (NoSuchElementException, TimeoutException):
                            company = "No company given"
                    except Exception as e:
                        print(f"An error occurred while finding company data: {e}")
                        company = "No company given"

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
                        WebDriverWait(driver, 30).until(lambda d: "@" in d.current_url)
                        map_url = driver.current_url
                        pattern = r"@(-?\d+\.\d+),(-?\d+\.\d+)"
                        match = re.search(pattern, map_url)
                        if match:
                            va_lat, va_long = match.groups()
                        else:
                            va_lat = "No lat given"
                            va_long = "No long given"
                    except (NoSuchElementException, TimeoutException) as e:
                        print("Map extraction error:", e)
                        va_lat = "No lat given"
                        va_long = "No long given"

                va_data = [
                        (col_company, company),
                        (col_salary, salary),
                        (col_address, address),
                        (col_lat, va_lat),
                        (col_long, va_long),
                        (col_tenure, tenure),
                        (col_closes, closes),
                        (col_description, job_description)
                ]
                batch_update_cells(va_sheet, row_num, va_data)
                time.sleep(3)
                progress["RowNum"] += 2
            progress["progress"] = "finished"
        except NoSuchElementException as e:
            print(f"Error processing detail: {e}")
            continue

    progress_sheet.update("A3", [[json.dumps({"progress": "setting", "UrlNum": 1})]])
    progress_sheet.update("A4", [[json.dumps({"progress": "setting", "RowNum": 0})]])
    va_sheet.update([["Scrapping Finished"]], "Q1")
    driver.quit()
    print("Saved every data into the Google Sheet successfully.")

    if __name__ == "__main__":
        main()

if __name__ == "__main__":
    main()
