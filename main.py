from urllib.parse import urljoin
from requests import ReadTimeout
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
spreadsheet_url = "https://docs.google.com/spreadsheets/d/13fIG9eUVVH1OKkQ6CaaTNSr1Cb8eUg-qCNXxm9m7eu0/edit?gid=0#gid=0"
credentials = Credentials.from_service_account_file(key_path, scopes=scopes)
gc = gspread.authorize(credentials)
spreadsheet = gc.open_by_url(spreadsheet_url)
URL = "https://www.yourcareer.gov.au/occupations?address%5Blocality%5D=&address%5Bstate%5D=VIC&address%5Bpostcode%5D=&address%5Blatitude%5D=0&address%5Blongitude%5D=0&address%5BformattedLocality%5D=Victoria%20%28VIC%29&distanceFilter=25&pageNumber="

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

class ProgressManager:
    def __init__(self, progress_sheet):
        self.progress_sheet = progress_sheet

    def _is_first_execution(self):
        try:
            progress_json = self.progress_sheet.acell("A1").value
            return not bool(progress_json)
        except Exception:
            return True

    def save_progress(self, progress):
        if self._is_first_execution():
            progress = {
                "Phase": "Scrapping",
                "finished": False,
                "UrlNum": 0,
                "OccIndex": 0
            }
        try:
            self.progress_sheet.update("A1", json.dumps(progress))
        except Exception:
            print("Failed to save progress.")

    def load_progress(self):
        try:
            if self._is_first_execution():
                progress = {
                    "Phase": "Scrapping",
                    "finished": False,
                    "UrlNum": 0,
                    "OccIndex": 0
                }
                return progress
            else:
                progress_json = self.progress_sheet.acell("A1").value
                if progress_json:
                    progress = json.loads(progress_json)
                    return progress
                else:
                    raise Exception("No progress value found in A1")
        except Exception:
            print("Failed to load progress, finishing program")
            return {"finished": True}

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
                print(f"Failed to append element {data} after {retries} attempts.")
                return

def get_worksheet(sheet_name):
    return spreadsheet.worksheet(sheet_name)

def set_occ_sheet():
    sheet_name = "Occupation"
    worksheet = spreadsheet.worksheet(sheet_name)
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
    worksheet = get_worksheet("OccupationData")
    worksheet.clear()
    headers = ["detail_url", "occupation_name", "num_vacancy", "vacancy_hyper_link", "courses_url_escaped"]
    worksheet.append_row(headers)
    return worksheet

def dict_to_row(data_dict):
    return [data_dict.get(key, "") for key in ["detail_url", "occupation_name", "num_vacancy", "vacancy_hyper_link", "courses_url_escaped"]]

def find_occupation_code(link):
    # find occupation code from url
    code = r"/occupations/(\d+)/"
    found = re.findall(code, link)
    return found

def overview_to_skills(link):
    # change overview tab url to skills tab url
    modified_link = re.sub(r"(\?|&)tab=overview", r"\1tab=skills", link)
    return modified_link

def is_first_execution(progress_sheet):
    progress_value = progress_sheet.acell("A1").value
    return not progress_value or progress_value.strip() == ""

def wait_for_page_load(driver, timeout=15):
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException:
        print("Page loading timeout.")
    except Exception as e:
        print(f"An error occurred while waiting for page load: {e}")

def get_occupation_data():
    sheet = get_worksheet("OccupationData")
    all_rows = sheet.get_all_values()
    if not all_rows:
        return []

    header = all_rows[0]
    occupation_list = []
    for row in all_rows[1:]:
        row_dict = {header[i]: row[i] if i < len(row) else "" for i in range(len(header))}
        occupation_list.append(row_dict)
    return occupation_list

def duplicate_list():
    occ_sheet = get_worksheet("Occupation")
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
        dup_list.append(occ_code)
    return dup_list

def scrapping(driver):
    wait = WebDriverWait(driver, 10)
    data_sheet = get_worksheet("OccupationData")

    progress_sheet = get_worksheet("Progress")
    if is_first_execution(progress_sheet):
        set_occ_sheet()
    progress_manager = ProgressManager(progress_sheet)
    progress = progress_manager.load_progress()
    url_num = progress.get("UrlNum", 1)
    while True:
        occ_url = URL + str(url_num)

        try:
            driver.get(occ_url)
        except Exception:
            print(f"Failed to load page: {occ_url}")
            url_num += 1
            continue

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        wait_for_page_load(driver)

        occ_index = progress.get("OccIndex", 0)  # reset OccIndex for this url
        set_occupation_data_sheet()
        while True:
            try:
                occupations = wait.until(EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "section[class='mint-search-result-item no-description']")))
            except TimeoutException:
                print(f"Vacancy elements for page {url_num} did not load in time.")
                break
            except Exception as e:
                print(f"An error occurred while waiting for page load: {e}")
                break

            while occ_index < len(occupations):
                occupation = occupations[occ_index]

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
                occupation_row = dict_to_row(occupation_data)
                append_row_with_retry(data_sheet, occupation_row)
                progress = {"Phase": "Scrapping", "finished": False, "UrlNum": url_num, "OccIndex": occ_index}
                progress_manager.save_progress(progress)
                occ_index += 1

        try:
            next_button = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "button[aria-label='Go to next page']")))
            driver.execute_script("arguments[0].click();", next_button)
            wait_for_page_load(driver)
            progress = {"Phase": "Scrapping", "finished": False, "UrlNum": url_num, "OccIndex": 0}
            progress_manager.save_progress(progress)
        except (NoSuchElementException, TimeoutException):
            progress = {"Phase": "Detail", "finished": False, "UrlNum": 1, "OccIndex": 0}
            progress_manager.save_progress(progress)
            break
        except Exception as e:
            print(f"An error occurred while finding next button: {e}")
            break

def detail(driver):
    occ_sheet = get_worksheet("Occupation")
    progress_sheet = get_worksheet("Progress")
    if is_first_execution(progress_sheet):
        set_occ_sheet()
    progress_manager = ProgressManager(progress_sheet)
    progress = progress_manager.load_progress()
    url_num = progress.get("UrlNum", 1)

    occ_index = progress.get("OccIndex", 0)  # reset OccIndex for this url
    occ_data = get_occupation_data()
    seen_jobs = set(duplicate_list() or [])
    while occ_index < len(occ_data):
        occ_element = occ_data[occ_index]
        # open detail page
        occ_detail_url = occ_element['detail_url']
        if occ_detail_url is None:
            print(f"Detail URL is None for {occ_element['occupation_name']}. Skipping this detail page.")
            occ_index += 1
            continue
        driver.get(occ_detail_url)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        if not wait_for_page_load(driver):
            print(f"Page load failed for detail page: {occ_detail_url}. Skipping this detail page.")
            occ_index += 1
            continue
        print(f"current page: {occ_element['occupation_name']}")
        current_url = driver.current_url
        codes = find_occupation_code(current_url)
        occupation_code = codes[0] if codes else "No code found"
        occupation_link = f'=HYPERLINK("{occ_detail_url}", "{occ_detail_url}")'

        if occ_detail_url is None:
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
            # find description
            try:
                description = driver.find_element(By.CSS_SELECTOR,
                                                       "div[class='text-lg']").text
            except NoSuchElementException:
                description = "No description given"

            # find average salary
            try:
                average_salary = driver.find_element(By.CSS_SELECTOR,
                                                          "h3[identifer='Occupation_Insights_Average_Salary'] ~ p").text
            except NoSuchElementException:
                average_salary = "No average salary given"

            # find future demand
            try:
                raw_future_demand = driver.find_element(By.CSS_SELECTOR,
                                                             "h3[identifer='Occupation_Insights_Future_Demand']")
                li_future_demand = raw_future_demand.find_element(By.XPATH, "./ancestor::li")
                future_demand = li_future_demand.find_element(By.CSS_SELECTOR,
                                                              "span[class='mint-pill__content-label']").text
            except NoSuchElementException:
                future_demand = "No future demand given"

            # find job type
            try:
                job_type = driver.find_element(By.CSS_SELECTOR,
                                                    "h3[identifer='Occupation_Insights_Job_Type'] ~ p").text
            except NoSuchElementException:
                job_type = "No job type given"

            # find skill level
            try:
                skill_level = driver.find_element(By.CSS_SELECTOR,
                                                       "h3[identifer='Occupation_Insights_Skill_Level'] ~ p").text
            except NoSuchElementException:
                skill_level = "No skill level given"

            # find industry
            try:
                raw_industry = driver.find_element(By.CSS_SELECTOR, "ul[class='industry-link-list']")
                industries = raw_industry.find_elements(By.CSS_SELECTOR, "a[class='mint-link']")
                industry_list = []
                for ind_element in industries:
                    industry_list.append(ind_element.text)
                industry = ", \n".join(industry_list)
            except NoSuchElementException:
                industry = "No industry given"

            # find Apprenticeships and traineeships
            try:
                raw_aat = driver.find_element(By.CSS_SELECTOR, "ul.list-inline")
                li_aat = raw_aat.find_elements(By.CSS_SELECTOR, "span.mint-pill__content-label")
                aat_list = [element.text for element in li_aat if element.text.strip() != '']
                aat = ", \n".join(aat_list) if aat_list else "No Apprenticeships and traineeships given"
            except NoSuchElementException:
                aat = "No Apprenticeships and traineeships given"

            # find interests
            try:
                interests = driver.find_element(By.CSS_SELECTOR, "h3[identifier='Interests_Stories_Heading'] ~ ul")
                overview_interests = interests.find_elements(By.CSS_SELECTOR, "span[class='mint-pill__content-label']")
                interests_list = []
                for interest in overview_interests:
                    interests_list.append(interest.text)
                overview_interests_text = ", \n".join(interests_list)
            except NoSuchElementException:
                overview_interests_text = "No interests given"

            # find considerations
            try:
                considerations = driver.find_element(By.CSS_SELECTOR,
                                                          "h3[identifier='Considerations_Stories_Heading'] ~ ul")
                overview_considerations = considerations.find_elements(By.CSS_SELECTOR,
                                                                       "span[class='mint-pill__content-label']")
                considerations_list = []
                for consideration in overview_considerations:
                    considerations_list.append(consideration.text)
                overview_considerations_text = ", \n".join(considerations_list)
            except NoSuchElementException:
                overview_considerations_text = "No considerations given"

            # find day-to-day overview
            try:
                dtds = driver.find_element(By.CSS_SELECTOR, "h3[identifier='Day_to_day_Stories_Heading'] ~ ul")
                dtd_elements = dtds.find_elements(By.TAG_NAME, "li")
                dtd_list = []
                for dtd_element in dtd_elements:
                    dtd_list.append(f"'{dtd_element.text}'")
                dtd = ",\n".join(dtd_list)
            except NoSuchElementException:
                dtd = "No day-to-day given"

            # find skills using skills tab
            try:
                current_url = driver.current_url
                skills_url = overview_to_skills(current_url)
                driver.get(skills_url)
                wait_for_page_load(driver)
                try:
                    skills = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR,
                                                      "p[identifier='Skills_Top_Skills_Requested'] ~ ul")))
                    skills_elements = skills.find_elements(By.CSS_SELECTOR, "span[class='mint-pill__content-label']")
                    skills_list = []
                    for skills_element in skills_elements:
                        skills_list.append(skills_element.get_attribute("textContent"))
                    skills_text = ", ".join(skills_list)
                except TimeoutException:
                    print("Skills section did not load in time.")
                except Exception:
                    skills_text = "No skills given"
            except NoSuchElementException:
                skills_text = "Failed to load skills page"

        occ_detail_url_escaped = occ_element['courses_url_escaped']
        courses_hyper_link = f'=HYPERLINK("{occ_detail_url_escaped}", "{occ_detail_url_escaped}")'

        occupation_detail = [occupation_code,
                             occ_element["occupation_name"],
                             occupation_link,
                             description,
                             average_salary,
                             future_demand,
                             job_type,
                             skill_level,
                             industry,
                             skills_text,
                             occ_element['num_vacancy'],
                             occ_element["vacancy_hyper_link"],
                             courses_hyper_link,
                             aat,
                             overview_interests_text,
                             overview_considerations_text,
                             dtd]
        append_row_with_retry(occ_sheet, occupation_detail)
        time.sleep(3)
        seen_jobs.add(occupation_code)
        occ_index += 1
        progress = {"Phase": "Detail", "finished": False, "UrlNum": url_num, "OccIndex": occ_index}
        progress_manager.save_progress(progress)

    progress = {"Phase": "Scrapping", "finished": True, "UrlNum": 1, "OccIndex": 0}
    progress_manager.save_progress(progress)

def main():
    # main scrapping function
    driver = set_driver()
    try:
        progress_sheet = get_worksheet("Progress")
        if is_first_execution(progress_sheet):
            set_occ_sheet()
        progress_manager = ProgressManager(progress_sheet)
        progress = progress_manager.load_progress()

        while not progress.get("finished"):
            phase = progress.get("Phase")

            if phase == "Scrapping":
                scrapping(driver)

            elif phase == "Detail":
                detail(driver)
            progress = progress_manager.load_progress()

        print("Finished scrapping occupation process.")

    finally:
        driver.quit()
        print("Saved every data into the Google Sheet successfully.")

if __name__ == "__main__":
    main()