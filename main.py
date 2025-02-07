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

scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
credentials = Credentials.from_service_account_file(key_path, scopes=scopes)
gc = gspread.authorize(credentials)
spreadsheet_url = "https://docs.google.com/spreadsheets/d/13fIG9eUVVH1OKkQ6CaaTNSr1Cb8eUg-qCNXxm9m7eu0/edit?gid=0#gid=0"
sh = gc.open_by_url(spreadsheet_url)

def get_worksheet(sheet_name):
    return sh.worksheet(sheet_name)
    
def load_progress_occ():
    progress_sheet = get_worksheet("Progress")
    progress_val = progress_sheet.acell("A2").value
    if not progress_val:
        return {
            "phase": "list",
            "page_num": 1,
            "detail_index": 0,
            "finished": False
        }
    try:
        progress = json.loads(progress_val)
        minimal_progress = {
            "phase": progress.get("phase", "list"),
            "page_num": progress.get("page_num", 1),
            "detail_index": progress.get("detail_index", 0),
            "finished": progress.get("finished", False)
        }
        return minimal_progress
    except Exception as e:
        print("Error loading occupation progress from sheet, using default:", e)
        return {
            "phase": "list",
            "page_num": 1,
            "detail_index": 0,
            "finished": False
        }

def save_progress_occ(progress):
    progress_sheet = get_worksheet("Progress")
    minimal_progress = {
        "phase": progress.get("phase", "list"),
        "page_num": progress.get("page_num", 1),
        "detail_index": progress.get("detail_index", 0),
        "finished": progress.get("finished", False)
    }
    progress_sheet.update_acell("A2", json.dumps(minimal_progress))

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
    worksheet = sh.worksheet(sheet_name)
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
        ws = sh.add_worksheet("OccupationData", rows="1000", cols="20")
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

    if progress["phase"] == "list":
        page_num = progress.get("page_num", 1)
        while True:
            url = f"https://www.yourcareer.gov.au/occupations?address%5Blocality%5D=&address%5Bstate%5D=VIC&address%5Bpostcode%5D=&address%5Blatitude%5D=0&address%5Blongitude%5D=0&address%5BformattedLocality%5D=Victoria%20%28VIC%29&distanceFilter=25&pageNumber={page_num}"
            driver.get(url)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)
            print(f"current page number: {page_num}")

            occupations = driver.find_elements(By.CSS_SELECTOR, "section[class='mint-search-result-item no-description']")
            time.sleep(3)
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
                occupation_data = {
                    "detail_url": detail_url,
                    "occupation_name": occupation_name,
                    "num_vacancy": num_vacancy,
                    "vacancy_hyper_link": vacancy_hyper_link,
                    "courses_url_escaped": courses_url_escaped
                }
                all_occupation_data.append(occupation_data)
            progress["page_num"] = page_num
            progress["all_occupation_data"] = all_occupation_data
            save_progress_occ(progress)
            if not check_next_button(driver):
                break
            page_num += 1
        save_occupation_data(all_occupation_data)
        progress["phase"] = "detail_extraction"
        progress["detail_index"] = 0
        save_progress_occ(progress)
    else:
        all_occupation_data = load_occupation_data()

    detail_index = progress.get("detail_index", 0)
    for i in range(detail_index, len(all_occupation_data)):
        occ_info = all_occupation_data[i]
        driver.get(occ_info["detail_url"])
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(5)
        current_url = driver.current_url
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
            print(f"Failed to find {occupation_name} link. Skipping...")
        else:
            try:
                description = driver.find_element(By.CSS_SELECTOR, "div[class='text-lg']").text
            except NoSuchElementException:
                description = "No description given"
            try:
                average_salary = driver.find_element(By.CSS_SELECTOR,"h3[identifer='Occupation_Insights_Average_Salary'] ~ p").text
            except NoSuchElementException:
                average_salary = "No average salary given"
            try:
                raw_future_demand = driver.find_element(By.CSS_SELECTOR, "h3[identifer='Occupation_Insights_Future_Demand']")
                li_future_demand = raw_future_demand.find_element(By.XPATH, "./ancestor::li")
                future_demand = li_future_demand.find_element(By.CSS_SELECTOR,"span[class='mint-pill__content-label']").text
            except NoSuchElementException:
                future_demand = "No future demand given"
            try:
                job_type = driver.find_element(By.CSS_SELECTOR,"h3[identifer='Occupation_Insights_Job_Type'] ~ p").text
            except NoSuchElementException:
                job_type = "No job type given"
            try:
                skill_level = driver.find_element(By.CSS_SELECTOR,"h3[identifer='Occupation_Insights_Skill_Level'] ~ p").text
            except NoSuchElementException:
                skill_level = "No skill level given"
            try:
                raw_industry = driver.find_element(By.CSS_SELECTOR, "ul[class='industry-link-list']")
                industries = raw_industry.find_elements(By.CSS_SELECTOR, "a[class='mint-link']")
                industry_list = []
                for ind_element in industries:
                    industry_list.append(ind_element.text)
                industry = ", \n".join(industry_list)
            except NoSuchElementException:
                industry = "No industry given"
            try:
                raw_aat = driver.find_element(By.CSS_SELECTOR, "ul.list-inline")
                li_aat = raw_aat.find_elements(By.CSS_SELECTOR, "span.mint-pill__content-label")
                aat_list = [element.text for element in li_aat if element.text.strip() != '']
                aat = ", \n".join(aat_list) if aat_list else "No Apprenticeships and traineeships given"
            except NoSuchElementException:
                aat = "No Apprenticeships and traineeships given"
            try:
                interests = driver.find_element(By.CSS_SELECTOR, "h3[identifier='Interests_Stories_Heading'] ~ ul")
                overview_interests = interests.find_elements(By.CSS_SELECTOR, "span[class='mint-pill__content-label']")
                interests_list = []
                for interest in overview_interests:
                    interests_list.append(interest.text)
                overview_interests_text = ", \n".join(interests_list)
            except NoSuchElementException:
                overview_interests_text = "No interests given"
            try:
                considerations = driver.find_element(By.CSS_SELECTOR,"h3[identifer='Considerations_Stories_Heading'] ~ ul")
                overview_considerations = considerations.find_elements(By.CSS_SELECTOR,"span[class='mint-pill__content-label']")
                considerations_list = []
                for consideration in overview_considerations:
                    considerations_list.append(consideration.text)
                overview_considerations_text = ", \n".join(considerations_list)
            except NoSuchElementException:
                overview_considerations_text = "No considerations given"
            try:
                dtds = driver.find_element(By.CSS_SELECTOR, "h3[identifer='Day_to_day_Stories_Heading'] ~ ul")
                dtd_elements = dtds.find_elements(By.TAG_NAME, "li")
                dtd_list = []
                for dtd_element in dtd_elements:
                    dtd_list.append(f"'{dtd_element.text}'")
                dtd = ",\n".join(dtd_list)
            except NoSuchElementException:
                dtd = "No day-to-day given"
            try:
                current_url = driver.current_url
                skills_url = overview_to_skills(current_url)
                driver.get(skills_url)
                time.sleep(5)
                try:
                    skills = driver.find_element(By.CSS_SELECTOR, "p[identifer='Skills_Top_Skills_Requested'] ~ ul")
                    skills_elements = skills.find_elements(By.CSS_SELECTOR, "span[class='mint-pill__content-label']")
                    skills_list = []
                    for skills_element in skills_elements:
                        skills_list.append(skills_element.get_attribute("textContent"))
                    skills_text = ", ".join(skills_list)
                except Exception:
                    skills_text = "No skills given"
            except NoSuchElementException:
                skills_text = "Failed to load skills page"
                
        occ_detail_url_escaped = occ_info['courses_url_escaped']
        courses_hyper_link = f'=HYPERLINK("{occ_detail_url_escaped}", "{occ_detail_url_escaped}")'
        occupation_detail = [occupation_code,
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
                             dtd]
        append_row_with_retry(va_sheet, occupation_detail)
        time.sleep(3)
        progress["detail_index"] = i + 1
        save_progress_occ(progress)
            
    progress["finished"] = True
    save_progress_occ(progress)
    driver.quit()
    print("Saved every data into the Google Sheet successfully.")

if __name__ == "__main__":
    main()
