# vacancy_scrapping.py
import datetime
import re
import time
from urllib.parse import urljoin  # join url

import gspread
from requests import ReadTimeout
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from google_form_package import Sheet
from process_handler import ProcessHandler

web_sheet = Sheet()
driver = web_sheet.set_driver()

def append_rows_with_retry(worksheet, data, retries=3, delay=5):
    for attempt in range(retries):
        try:
            worksheet.append_rows(data, value_input_option="USER_ENTERED")
            return
        except gspread.exceptions.APIError as e:
            if any(code in str(e) for code in ["500", "502", "503", "504","429"]) or isinstance(e, ReadTimeout):
                print(f"Error occurred. Retry after {delay} seconds ({attempt+1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                print(f"Failed to append element {data} after {retries} attempts.")
                return

def set_vacancy_sheet():
    # set for vacancy sheet
    worksheet = web_sheet.get_worksheet("Vacancies")
    worksheet.clear()
    headers = ["occupation", "occupation link", "date added", "time scrapped", "job title", "job link", "job code", "company", "salary",
               "address", "lat", "long", "tenure", "overview", "closes", "description"]
    worksheet.append_row(headers)
    return worksheet

def set_vacancy_data_sheet():
    # set for VacancyData sheet
    worksheet = web_sheet.get_worksheet("VacancyData")
    worksheet.clear()
    headers = ["job code"]
    worksheet.append_row(headers)
    return worksheet

def load_to_seen_data():
    vac_sheet = web_sheet.get_worksheet("Vacancies")
    data_sheet = web_sheet.get_worksheet("VacancyData")
    data_sheet.clear()
    vac_header = vac_sheet.row_values(1)
    try:
        vac_code_idx = vac_header.index("job code") + 1
    except ValueError as e:
        print("Could not detect requested row", e)
        return
    all_rows = vac_sheet.get_all_values()[1:]
    dup_list = []
    for row_num, row in enumerate(all_rows, start=2):
        vac_code = row[vac_code_idx - 1] if len(row) >= vac_code_idx else ""
        dup_list.append([vac_code])
    if dup_list:
        data_sheet.append_rows(dup_list, value_input_option="USER_ENTERED")

def save_seen_jobs_data(worksheet, seen_jobs):
    rows = [[job_code] for job_code in seen_jobs]
    worksheet.append_rows(rows, value_input_option="USER_ENTERED")

def load_seen_jobs_data(worksheet):
    seen_jobs = set()
    rows = worksheet.get_all_values()
    for row in rows[1:]:
        if row and len(row) >= 1:
            seen_jobs.add(row[0].strip().lower())
    return seen_jobs

def wait_for_page_load(web_driver, timeout=15):
    try:
        WebDriverWait(web_driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException:
        print("Page loading timeout.")
    except Exception as e:
        print(f"An error occurred while waiting for page load: {e}")

def main():
    wait = WebDriverWait(driver, 10)
    vac_sheet = web_sheet.get_worksheet("Vacancies")
    data_sheet = web_sheet.get_worksheet("VacancyData")
    progress_sheet = web_sheet.get_worksheet("Progress")
    load_to_seen_data()
    seen_jobs = load_seen_jobs_data(data_sheet)
    ph = ProcessHandler(progress_sheet, {"progress": "setting", "UrlNum": 1}, "A3",
                        shutdown_callback=lambda: save_seen_jobs_data(data_sheet, seen_jobs))
    progress = ph.load_progress()
    if progress["progress"] == "setting":
        set_vacancy_sheet()
        set_vacancy_data_sheet()
    url_num = progress.get("UrlNum", 1)
    vac_sheet.update([["Running Scrapping"]], "Q1")

    buffer = []
    while not progress["progress"] == "finished":
        try:
            progress["progress"] = "processing"
            va_url = "https://www.workforceaustralia.gov.au/individuals/jobs/search?locationCodes%5B0%5D=7&jobAge=3&pageNumber=" + str(progress["UrlNum"])
            driver.get(va_url)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            wait_for_page_load(driver)
            print(f"current page: {va_url}")
            progress['UrlNum'] += 1

            try:
                vacancies = wait.until(EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "section.mint-search-result-item.has-img.has-actions.has-preheading")))
            except TimeoutException:
                print(f"Vacancy elements for page {url_num} did not load in time.")
                break
            except Exception as e:
                print(f"An error occurred while waiting for page load: {e}")
                break

            for vacancy in vacancies:
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
                    date_added = (today - datetime.timedelta(days=date_added_dif)).strftime("%B %d, %Y")
                except NoSuchElementException:
                    date_added = "No date added given"

                time_scrapped = datetime.datetime.now().strftime("%B %d, %Y %I:%M %p")

                try:
                    overview = vacancy.find_element(By.CSS_SELECTOR, "div.mint-blurb").text
                except NoSuchElementException:
                    overview = "No overview given"

                vacancy_data = ["",
                                "",
                                str(date_added),
                                str(time_scrapped),
                                job_title,
                                job_link,
                                job_code,
                                "",
                                "",
                                "",
                                "",
                                "",
                                "",
                                overview,
                                "",
                                ""
                ]
                buffer.append(vacancy_data)
                seen_jobs.add(job_code)
                time.sleep(1)
                
                if len(buffer) >= 20:
                    append_rows_with_retry(vac_sheet, buffer)
                    buffer = []

            if buffer:
                append_rows_with_retry(vac_sheet, buffer)
                buffer = []
                
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

        except NoSuchElementException as e:
            print(f"Error processing job: {e}")
            progress['UrlNum'] += 1
            continue

    set_vacancy_data_sheet()
    driver.quit()
    print("Saved every data into the Google Sheet successfully.")


if __name__ == "__main__":
    main()

