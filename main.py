import os
import json
import time
import re
from urllib.parse import urljoin

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from google.oauth2.service_account import Credentials
import gspread
from gspread_formatting import *

def load_progress_occ(progress_sheet):
    try:
        progress_json = progress_sheet.acell("A1").value
        if progress_json:
            progress = json.loads(progress_json)
            progress.pop("all_occupation_data", None)
            return progress
        else:
            raise Exception("No progress value found in A1")
    except Exception:
        return {
            "phase": "list",
            "page_num": 1,
            "detail_index": 0,
            "finished": False
        }

def save_progress_occ(progress_sheet, progress):
    progress_sheet.update(values=[[json.dumps(progress)]], range_name="A1")

def set_sheets():
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
    credentials = Credentials.from_service_account_file(key_path, scopes=scopes)
    gc = gspread.authorize(credentials)
    spreadsheet_url = "https://docs.google.com/spreadsheets/d/13fIG9eUVVH1OKkQ6CaaTNSr1Cb8eUg-qCNXxm9m7eu0/edit?gid=0#gid=0"
    sh = gc.open_by_url(spreadsheet_url)

    occ_sheet_name = "Occupation"
    occupation_sheet = sh.worksheet(occ_sheet_name)
    occupation_sheet.clear()
    occ_headers = [
        "occupation code", "occupation", "occupation link", "description", "average salary",
        "future demand", "job type", "skill level", "industry", "skills", "number of vacancies",
        "link to vacancies", "link to courses", "apprenticeships and traineeships",
        "overview : interests", "overview : considerations", "overview : day-to-day"
    ]
    occupation_sheet.append_row(occ_headers)
    header_format = CellFormat(
        backgroundColor=Color(0.8, 1, 0.8),
        textFormat=TextFormat(bold=True, fontSize=12),
        horizontalAlignment='CENTER'
    )
    format_cell_range(occupation_sheet, 'A1:Q1', header_format)
    for col in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'I', 'K', 'L', 'M', 'H']:
        set_column_width(occupation_sheet, col, 150)
    for col in ['J', 'N', 'O', 'Q']:
        set_column_width(occupation_sheet, col, 200)
    for col in ['P']:
        set_column_width(occupation_sheet, col, 300)

    try:
        progress_sheet = sh.worksheet("Progress")
    except gspread.exceptions.WorksheetNotFound:
        progress_sheet = sh.add_worksheet("Progress", rows="100", cols="10")
    
    try:
        data_sheet = sh.worksheet("OccupationData")
    except gspread.exceptions.WorksheetNotFound:
        data_sheet = sh.add_worksheet("OccupationData", rows="1000", cols="10")
    data_sheet.clear()
    data_headers = ["detail_url", "occupation_name", "num_vacancy", "vacancy_hyper_link", "courses_url_escaped"]
    data_sheet.append_row(data_headers)
    
    return occupation_sheet, progress_sheet, data_sheet

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
            if "503" in str(e) or "500" in str(e):
                print(f"Error 503 occurred. Retry after {delay} seconds ({attempt+1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                raise

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

def main():
    occupation_sheet, progress_sheet, data_sheet = set_sheets()
    progress = load_progress_occ(progress_sheet)
    
    if progress.get("finished", False):
        print("Process already finished.")
        return

    page_driver = set_driver()

    if progress["phase"] == "list":
        page_num = progress.get("page_num", 1)
        while True:
            url = (f"https://www.yourcareer.gov.au/occupations?address%5Blocality%5D=&"
                   f"address%5Bstate%5D=VIC&address%5Bpostcode%5D=&address%5Blatitude%5D=0&"
                   f"address%5Blongitude%5D=0&address%5BformattedLocality%5D=Victoria%20%28VIC%29&"
                   f"distanceFilter=25&pageNumber={page_num}")
            page_driver.get(url)
            page_driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)
            print(f"current page number: {page_num}")

            occupations = page_driver.find_elements(By.CSS_SELECTOR, "section[class='mint-search-result-item no-description']")
            time.sleep(3)
            rows_to_append = []
            for occupation in occupations:
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

                courses_links = occupation.find_elements(By.CSS_SELECTOR, "a[aria-label^='Explore courses'], a[aria-label^='View course']")
                if courses_links:
                    courses_url = courses_links[0].get_attribute("href")
                    courses_url_escaped = courses_url.replace('"', '\\"')
                else:
                    courses_url_escaped = "No link given"

                try:
                    detail_link = occupation.find_element(By.CSS_SELECTOR, "a[class='link mint-link link']")
                    occupation_name = detail_link.text
                    detail_url = detail_link.get_attribute("href")
                except NoSuchElementException:
                    occupation_name = "No occupation name given"
                    detail_url = None

                row = [detail_url, occupation_name, num_vacancy, vacancy_hyper_link, courses_url_escaped]
                rows_to_append.append(row)

            if rows_to_append:
                try:
                    data_sheet.append_rows(rows_to_append, value_input_option="USER_ENTERED")
                except Exception as e:
                    print("Error appending rows:", e)

            progress["page_num"] = page_num
            save_progress_occ(progress_sheet, progress)
            if not check_next_button(driver=page_driver):
                break
            page_num += 1
        
        progress["phase"] = "details"
        progress["detail_index"] = 0
        save_progress_occ(progress_sheet, progress)
    else:
        records = data_sheet.get_all_records()
        all_occupation_data = []
        for record in records:
            all_occupation_data.append({
                "detail_url": record["detail_url"],
                "occupation_name": record["occupation_name"],
                "num_vacancy": record["num_vacancy"],
                "vacancy_hyper_link": record["vacancy_hyper_link"],
                "courses_url_escaped": record["courses_url_escaped"]
            })
    
    if progress["phase"] == "details" and 'all_occupation_data' not in locals():
        records = data_sheet.get_all_records()
        all_occupation_data = []
        for record in records:
            all_occupation_data.append({
                "detail_url": record["detail_url"],
                "occupation_name": record["occupation_name"],
                "num_vacancy": record["num_vacancy"],
                "vacancy_hyper_link": record["vacancy_hyper_link"],
                "courses_url_escaped": record["courses_url_escaped"]
            })

    detail_index = progress.get("detail_index", 0)
    for i in range(detail_index, len(all_occupation_data)):
        occ_info = all_occupation_data[i]

        page_driver.get(occ_info["detail_url"])
        page_driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(5)
        current_url = page_driver.current_url
        print(f"current page: {occ_info['occupation_name']}")
        codes = find_occupation_code(current_url)
        occupation_code = codes[0] if codes else "No code found"
        occ_detail_url = occ_info['detail_url']
        occupation_link = f'=HYPERLINK("{occ_detail_url}", "{occ_detail_url}")'

        if occ_info["detail_url"] is None:
            description = "Failed to find description"
            occupation_name = "Failed to find name"
            occupation_code = "Failed to load detail page"
            average_salary = "Failed to load detail page"
            future_demand = "Failed to load detail page"
            job_type = "Failed to load detail page"
            skill_level = "Failed to load detail page"
            industry = "Failed to load detail page"
            overview_interests_text = "Failed to load detail page"
            overview_considerations_text = "Failed to load detail page"
            dtd = "Failed to load detail page"
            skills_text = "Failed to load detail page"
            aat = "Failed to load detail page"
            print(f"Failed to find {occ_info['occupation_name']} link. Skipping...")
        else:
            try:
                description = page_driver.find_element(By.CSS_SELECTOR, "div[class='text-lg']").text
            except NoSuchElementException:
                description = "No description given"

            try:
                average_salary = page_driver.find_element(By.CSS_SELECTOR,
                                                          "h3[identifer='Occupation_Insights_Average_Salary'] ~ p").text
            except NoSuchElementException:
                average_salary = "No average salary given"

            try:
                raw_future_demand = page_driver.find_element(By.CSS_SELECTOR, "h3[identifer='Occupation_Insights_Future_Demand']")
                li_future_demand = raw_future_demand.find_element(By.XPATH, "./ancestor::li")
                future_demand = li_future_demand.find_element(By.CSS_SELECTOR, "span[class='mint-pill__content-label']").text
            except NoSuchElementException:
                future_demand = "No future demand given"

            try:
                job_type = page_driver.find_element(By.CSS_SELECTOR,
                                                      "h3[identifer='Occupation_Insights_Job_Type'] ~ p").text
            except NoSuchElementException:
                job_type = "No job type given"

            try:
                skill_level = page_driver.find_element(By.CSS_SELECTOR,
                                                         "h3[identifer='Occupation_Insights_Skill_Level'] ~ p").text
            except NoSuchElementException:
                skill_level = "No skill level given"

            try:
                raw_industry = page_driver.find_element(By.CSS_SELECTOR, "ul[class='industry-link-list']")
                industries = raw_industry.find_elements(By.CSS_SELECTOR, "a[class='mint-link']")
                industry_list = [ind_element.text for ind_element in industries]
                industry = ", \n".join(industry_list)
            except NoSuchElementException:
                industry = "No industry given"

            try:
                raw_aat = page_driver.find_element(By.CSS_SELECTOR, "ul.list-inline")
                li_aat = raw_aat.find_elements(By.CSS_SELECTOR, "span.mint-pill__content-label")
                aat_list = [element.text for element in li_aat if element.text.strip() != '']
                aat = ", \n".join(aat_list) if aat_list else "No Apprenticeships and traineeships given"
            except NoSuchElementException:
                aat = "No Apprenticeships and traineeships given"

            try:
                interests = page_driver.find_element(By.CSS_SELECTOR, "h3[identifier='Interests_Stories_Heading'] ~ ul")
                overview_interests = interests.find_elements(By.CSS_SELECTOR, "span[class='mint-pill__content-label']")
                interests_list = [interest.text for interest in overview_interests]
                overview_interests_text = ", \n".join(interests_list)
            except NoSuchElementException:
                overview_interests_text = "No interests given"

            try:
                considerations = page_driver.find_element(By.CSS_SELECTOR,
                                                            "h3[identifier='Considerations_Stories_Heading'] ~ ul")
                overview_considerations = considerations.find_elements(By.CSS_SELECTOR,
                                                                       "span[class='mint-pill__content-label']")
                considerations_list = [consideration.text for consideration in overview_considerations]
                overview_considerations_text = ", \n".join(considerations_list)
            except NoSuchElementException:
                overview_considerations_text = "No considerations given"

            try:
                dtds = page_driver.find_element(By.CSS_SELECTOR, "h3[identifier='Day_to_day_Stories_Heading'] ~ ul")
                dtd_elements = dtds.find_elements(By.TAG_NAME, "li")
                dtd_list = [f"'{dtd_element.text}'" for dtd_element in dtd_elements]
                dtd = ",\n".join(dtd_list)
            except NoSuchElementException:
                dtd = "No day-to-day given"

            try:
                current_url = page_driver.current_url
                skills_url = overview_to_skills(current_url)
                page_driver.get(skills_url)
                time.sleep(5)
                try:
                    skills = page_driver.find_element(By.CSS_SELECTOR,
                                                        "p[identifier='Skills_Top_Skills_Requested'] ~ ul")
                    skills_elements = skills.find_elements(By.CSS_SELECTOR, "span[class='mint-pill__content-label']")
                    skills_list = [se.get_attribute("textContent") for se in skills_elements]
                    skills_text = ", ".join(skills_list)
                except Exception:
                    skills_text = "No skills given"
            except NoSuchElementException:
                skills_text = "Failed to load skills page"
                
        occ_detail_url_escaped = occ_info['courses_url_escaped']
        courses_hyper_link = f'=HYPERLINK("{occ_detail_url_escaped}", "{occ_detail_url_escaped}")'

        occupation_detail = [
            occupation_code,
            occ_info["occupation_name"],
            occupation_link,
            description,
            average_salary,
            future_demand,
            job_type,
            skill_level,
            industry,
            skills_text,
            occ_info['num_vacancy'],
            occ_info["vacancy_hyper_link"],
            courses_hyper_link,
            aat,
            overview_interests_text,
            overview_considerations_text,
            dtd
        ]
        append_row_with_retry(occupation_sheet, occupation_detail)
        time.sleep(3)
        progress["detail_index"] = i + 1
        save_progress_occ(progress_sheet, progress)

    progress["finished"] = True
    save_progress_occ(progress_sheet, progress)
    
    page_driver.quit()
    print("Saved every data into the Google Sheet successfully.")

if __name__ == "__main__":
    main()
