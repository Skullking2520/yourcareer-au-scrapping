# occupation_scrapping.py
import time

import gspread
from gspread_formatting import *
from requests import ReadTimeout
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
            if any(code in str(e) for code in ["500", "502", "503", "504","429"]) or isinstance(e, ReadTimeout):
                print(f"Error occurred. Retry after {delay} seconds ({attempt+1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                print(f"Failed to append element {data} after {retries} attempts.")
                return

def set_occ_sheet():
    worksheet = web_sheet.get_worksheet("Occupation")
    worksheet.clear()
    headers = ["occupation code", "occupation", "occupation link", "description", "average salary", "future demand",
               "job type",
               "skill level", "industry", "skills", "number of vacancies",
               "link to vacancies", "link to courses", "apprenticeships and traineeships",
               "overview : interests", "overview : considerations", "overview : day-to-day"]
    worksheet.append_row(headers)
    header_format = CellFormat(backgroundColor=Color(0.8, 1, 0.8), textFormat=TextFormat(bold=True, fontSize=12),
                               horizontalAlignment='CENTER')
    format_cell_range(worksheet, 'A1:Q1', header_format)
    for col in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'I', 'K', 'L', 'M', 'H']:
        set_column_width(worksheet, col, 150)
    for col in ['J', 'N', 'O', 'Q']:
        set_column_width(worksheet, col, 200)
    for col in ['P']:
        set_column_width(worksheet, col, 300)
    return worksheet

def set_occupation_data_sheet():
    # set for OccupationData sheet
    worksheet = web_sheet.get_worksheet("OccupationData")
    worksheet.clear()
    headers = ["occupation code"]
    worksheet.append_row(headers)
    return worksheet

def find_occupation_code(link):
    # find occupation code from url
    code = r"/occupations/(\d+)/"
    found = re.findall(code, link)
    return found

def wait_for_page_load(driver, timeout=15):
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException:
        print("Page loading timeout.")
    except Exception as e:
        print(f"An error occurred while waiting for page load: {e}")

def load_to_seen_data():
    occ_sheet = web_sheet.get_worksheet("Occupation")
    occ_header = occ_sheet.row_values(1)
    try:
        occ_code_idx = occ_header.index("occupation code") + 1
    except ValueError as e:
        print("Could not detect requested row", e)
        return
    all_rows = occ_sheet.get_all_values()[1:]
    dup_list = []
    for row_num, row in enumerate(all_rows, start=2):
        occ_code = row[occ_code_idx - 1] if len(row) >= occ_code_idx else ""
        dup_list.append([occ_code])
    data_sheet = web_sheet.get_worksheet("OccupationData")
    if dup_list:
        data_sheet.append_rows(dup_list, value_input_option="USER_ENTERED")

def save_seen_jobs_data(worksheet, seen_jobs):
    rows = [[occupation_code] for occupation_code in seen_jobs]
    worksheet.append_rows(rows, value_input_option="USER_ENTERED")

def load_seen_jobs_data(worksheet):
    seen_jobs = set()
    rows = worksheet.get_all_values()
    for row in rows[1:]:
        if row and len(row) >= 1:
            seen_jobs.add(row[0].strip().lower())
    return seen_jobs

def main():
    wait = WebDriverWait(driver, 10)
    occ_sheet = web_sheet.get_worksheet("Occupation")
    data_sheet = web_sheet.get_worksheet("OccupationData")
    progress_sheet = web_sheet.get_worksheet("Progress")
    load_to_seen_data()
    seen_jobs = load_seen_jobs_data(data_sheet)
    ph = ProcessHandler(progress_sheet, {"progress":"setting", "UrlNum":1}, "A1", shutdown_callback=lambda: save_seen_jobs_data(data_sheet, seen_jobs))
    progress = ph.load_progress()
    if progress["progress"] == "setting":
        set_occ_sheet()
        set_occupation_data_sheet()
    occ_sheet.update([["Running Scrapping"]], "R1")
    while not progress["progress"] == "finished":
        try:
            progress["progress"] = "progressing"
            url = f"https://www.yourcareer.gov.au/occupations?address%5Blocality%5D=&address%5Bstate%5D=VIC&address%5Bpostcode%5D=&address%5Blatitude%5D=0&address%5Blongitude%5D=0&address%5BformattedLocality%5D=Victoria%20%28VIC%29&distanceFilter=25&pageNumber={progress['UrlNum']}"
            driver.get(url)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            wait_for_page_load(driver)
            print(f"current page: {url}")
            progress['UrlNum'] += 1

            try:
                occupations = wait.until(EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "section[class='mint-search-result-item no-description']")))
            except TimeoutException:
                print(f"Vacancy elements for page {progress['UrlNum']} did not load in time.")
                break
            except Exception as e:
                print(f"An error occurred while waiting for page load: {e}")
                break

            for occupation in occupations:

                # find link to vacancies
                try:
                    vacancy_link = occupation.find_element(By.CSS_SELECTOR, "a[rel='nofollow']")
                    vacancy_url = vacancy_link.get_attribute("href")
                    vacancy_url_escaped = vacancy_url.replace('"', '\\"')
                    vacancy_hyper_link = f'=HYPERLINK("{vacancy_url_escaped}", "{vacancy_url_escaped}")'
                except NoSuchElementException:
                    vacancy_hyper_link = "No link given"

                try:
                    raw_num_vacancy = occupation.find_element(By.CSS_SELECTOR, "a[target='_blank']").text
                    match = re.search(r"^\d+", raw_num_vacancy)
                    if match:
                        num_vacancy = match.group()
                    else:
                        num_vacancy = "No number of vacancy given"
                except NoSuchElementException:
                    num_vacancy = "No number of vacancy given"

                # find link to courses
                courses_links = occupation.find_elements(By.CSS_SELECTOR,
                                                         "a[aria-label^='Explore courses'], a[aria-label^='View course']")
                if courses_links:
                    courses_url = courses_links[0].get_attribute("href")
                    courses_url_escaped = courses_url.replace('"', '\\"')
                else:
                    courses_url_escaped = "No link given"

                # move to detailed page
                try:
                    detail_link = occupation.find_element(By.CSS_SELECTOR, "a[class='link mint-link link']")

                    # find occupation name
                    occupation_name = detail_link.text

                    # find detail page url
                    detail_url = detail_link.get_attribute("href")

                    occupation_link = detail_url
                    occupation_hyper_link = f'=HYPERLINK("{occupation_link}", "{occupation_link}")'

                except NoSuchElementException:
                    occupation_name = "No occupation name given"
                    detail_url = None
                    occupation_hyper_link = "No detail url given"

                codes = find_occupation_code(detail_url)
                occupation_code = codes[0] if codes else "No code found"

                courses_hyper_link = f'=HYPERLINK("{courses_url_escaped}", "{courses_url_escaped}")'

                occupation_data = [
                    occupation_code,
                    occupation_name,
                    occupation_hyper_link,
                    "", # description
                    "", # average salary
                    "", # future demand
                    "", # job type
                    "", # skill level
                    "", # industry
                    "", # skills
                    num_vacancy,
                    vacancy_hyper_link,
                    courses_hyper_link,
                    "", # apprenticeships and traineeships
                    "", # overview : interests
                    "", # overview : considerations
                    "" # overview : day-to-day
                ]
                append_row_with_retry(occ_sheet, occupation_data)
                seen_jobs.add(occupation_code)
                time.sleep(1)
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
    time.sleep(5)
    set_occupation_data_sheet()
    driver.quit()
    print("Saved every data into the Google Sheet successfully.")

if __name__ == "__main__":
    main()
