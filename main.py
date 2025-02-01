from selenium import webdriver # web scrapping
from selenium.common.exceptions import NoSuchElementException,TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.oauth2.service_account import Credentials # google doc
from gspread_formatting import *
import time
import re
import gspread
import os

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

def append_row_with_retry(worksheet, data, retries=3, delay=5):
    for attempt in range(retries):
        try:
            worksheet.append_row(data, value_input_option="USER_ENTERED")
            return
        except gspread.exceptions.APIError as e:
            if "503" in error_message or "500" in str(e):
                print(f"Error 503 occurred. Retry after {delay}seconds ({attempt+1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                raise

def set_sheet():
    # This is for GitHub action
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

    sheet_name = "Occupation"
    worksheet = sh.worksheet(sheet_name)
    worksheet.clear()
    headers = ["occupation code", "occupation", "occupation link", "description", "average salary", "future demand", "job type",
               "skill level", "industry", "skills", "number of vacancies", "number of courses",
               "link to vacancies", "link to courses",
         "overview : interests", "overview : considerations", "overview : day-to-day"]
    worksheet.append_row(headers)
    header_format = CellFormat(backgroundColor=Color(0.8, 1, 0.8),textFormat=TextFormat(bold=True, fontSize=12),horizontalAlignment='CENTER')
    format_cell_range(worksheet, 'A1:M1', header_format)
    for col in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'K','L','H']:
        set_column_width(worksheet, col, 150)
    for col in ['M', 'N']:
        set_column_width(worksheet, col, 200)
    for col in ['J', 'P', 'Q']:
        set_column_width(worksheet, col, 300)
    return worksheet

def check_next_button(driver):
    # check if next button is there
    next_button = driver.find_elements(By.CSS_SELECTOR, "button[aria-label='Go to next page']")
    return bool(next_button)

def find_occupation_code(link):
    # find occupation code from url
    code = r"/occupations/(\d+)/"
    found = re.findall(code, link)
    return found

def overview_to_skills(link):
    # change overview tab url to skills tab url
    modified_link = re.sub(r"(\?|&)tab=overview", r"\1tab=skills", link)
    return modified_link

def main():
    # main scrapping function
    page_driver = set_driver()
    sheet = set_sheet()

    all_occupation_data = []

    page_num = 1

    while True:
        # get occupation detail in base page
        url = f"https://www.yourcareer.gov.au/occupations?address%5Blocality%5D=&address%5Bstate%5D=VIC&address%5Bpostcode%5D=&address%5Blatitude%5D=0&address%5Blongitude%5D=0&address%5BformattedLocality%5D=Victoria%20%28VIC%29&distanceFilter=25&pageNumber={page_num}"
        page_driver.get(url)
        page_driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        print(f"current page number: {page_num}")

        occupations = page_driver.find_elements(By.CSS_SELECTOR, "section[class='mint-search-result-item no-description']")
        time.sleep(3)
        for occupation in occupations:

            # find link to vacancies
            try:
                vacancy_link = occupation.find_element(By.CSS_SELECTOR, "a[rel='nofollow']")
                vacancy_url = vacancy_link.get_attribute("href")
                vacancy_url_escaped = vacancy_url.replace('"', '\\"')
                vacancy_hyper_link = f'=HYPERLINK("{vacancy_url_escaped}", "{vacancy_url_escaped}")'
            except NoSuchElementException:
                vacancy_hyper_link = "No link given"

            # find number of vacancies
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
            courses_links = occupation.find_elements(By.CSS_SELECTOR, "a[aria-label^='Explore courses'], a[aria-label^='View course']")
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
            all_occupation_data.append(occupation_data)
        if not check_next_button(driver=page_driver):
            break
        page_num += 1

    # get detail from page and add it to google dox
    for occ_info in all_occupation_data:

        # open detail page
        page_driver.get(occ_info["detail_url"])
        page_driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        current_url = page_driver.current_url
        print(f"current page: {occ_info['occupation_name']}")
        occupation_code = find_occupation_code(current_url)[0]

        occupation_link = f'=HYPERLINK("{occ_info['detail_url']}", "{occ_info['detail_url']}")'

        if occ_info["detail_url"] is None:
            description = "Failed to find description"
            occupation_name = "Failed to find name"
            occupation_code = "Failed to load detail page"
            average_salary = "Failed to load detail page"
            future_demand = "Failed to load detail page"
            job_type = "Failed to load detail page"
            skill_level = "Failed to load detail page"
            industry = "Failed to load detail page"
            num_courses = "Failed to load number of courses page"
            overview_interests_text = "Failed to load detail page"
            overview_considerations_text = "Failed to load detail page"
            dtd = "Failed to load detail page"
            skills_text = "Failed to load detail page"
            print(f"Failed to find {occupation_name} link. Skipping...")

        else:
            # find description
            try:
                description = page_driver.find_element(By.CSS_SELECTOR,
                                                          "div[class='text-lg']").text
            except NoSuchElementException:
                description = "No description given"

            # find average salary
            try:
                average_salary = page_driver.find_element(By.CSS_SELECTOR,"h3[identifer='Occupation_Insights_Average_Salary'] ~ p").text
            except NoSuchElementException:
                average_salary = "No average salary given"

            # find future demand
            try:
                raw_future_demand = page_driver.find_element(By.CSS_SELECTOR, "h3[identifer='Occupation_Insights_Future_Demand']")
                li_future_demand = raw_future_demand.find_element(By.XPATH, "./ancestor::li")
                future_demand = li_future_demand.find_element(By.CSS_SELECTOR,"span[class='mint-pill__content-label']").text
            except NoSuchElementException:
                future_demand = "No future demand given"

            # find job type
            try:
                job_type = page_driver.find_element(By.CSS_SELECTOR,"h3[identifer='Occupation_Insights_Job_Type'] ~ p").text
            except NoSuchElementException:
                job_type = "No job type given"

            # find skill level
            try:
                skill_level = page_driver.find_element(By.CSS_SELECTOR,"h3[identifer='Occupation_Insights_Skill_Level'] ~ p").text
            except NoSuchElementException:
                skill_level = "No skill level given"

            # find industry
            try:
                raw_industry = page_driver.find_element(By.CSS_SELECTOR, "ul[class='industry-link-list']")
                industries = raw_industry.find_elements(By.CSS_SELECTOR, "a[class='mint-link']")
                industry_list = []
                for ind_element in industries:
                    industry_list.append(ind_element.text)
                industry = ", \n".join(industry_list)
            except NoSuchElementException:
                industry = "No industry given"

            # find Apprenticeships and traineeships
            try:
                raw_aat = page_driver.find_element(By.CSS_SELECTOR, "ul[class='list-inline']")
                li_aat = raw_aat.find_elements(By.CSS_SELECTOR, "a[class='mint-link']")
                aat_list = []
                for aat_element in li_aat:
                    aat_list.append(aat_element.text)
                aat = ", \n".join(aat_list)
            except NoSuchElementException:
                aat = "No Apprenticeships and traineeships given"


            # find interests
            try:
                interests = page_driver.find_element(By.CSS_SELECTOR, "h3[identifier='Interests_Stories_Heading'] ~ ul")
                overview_interests = interests.find_elements(By.CSS_SELECTOR, "span[class='mint-pill__content-label']")
                interests_list = []
                for interest in overview_interests:
                    interests_list.append(interest.text)
                overview_interests_text = ", \n".join(interests_list)
            except NoSuchElementException:
                overview_interests_text = "No interests given"

            # find considerations
            try:
                considerations = page_driver.find_element(By.CSS_SELECTOR,
                                                          "h3[identifier='Considerations_Stories_Heading'] ~ ul")
                overview_considerations = considerations.find_elements(By.CSS_SELECTOR,"span[class='mint-pill__content-label']")
                considerations_list = []
                for consideration in overview_considerations:
                    considerations_list.append(consideration.text)
                overview_considerations_text = ", \n".join(considerations_list)
            except NoSuchElementException:
                overview_considerations_text = "No considerations given"

            # find day-to-day overview
            try:
                dtds = page_driver.find_element(By.CSS_SELECTOR, "h3[identifier='Day_to_day_Stories_Heading'] ~ ul")
                dtd_elements = dtds.find_elements(By.TAG_NAME, "li")
                dtd_list = []
                for dtd_element in dtd_elements:
                    dtd_list.append(f"'{dtd_element.text}'")
                dtd = ",\n".join(dtd_list)
            except NoSuchElementException:
                dtd = "No day-to-day given"

            # find skills using skills tab
            try:
                current_url = page_driver.current_url
                skills_url = overview_to_skills(current_url)
                page_driver.get(skills_url)
                time.sleep(5)
                try:
                    skills = page_driver.find_element(By.CSS_SELECTOR, "p[identifier='Skills_Top_Skills_Requested'] ~ ul")
                    skills_elements = skills.find_elements(By.CSS_SELECTOR, "span[class='mint-pill__content-label']")
                    skills_list = []
                    for skills_element in skills_elements:
                        skills_list.append(skills_element.get_attribute("textContent"))
                    skills_text = ", ".join(skills_list)
                except Exception:
                    skills_text = "No skills given"
            except NoSuchElementException:
                skills_text = "Failed to load skills page"

            # find number of courses
            try:
                base_url = "https://www.yourcareer.gov.au"
                full_courses_url = urljoin(base_url, occ_info['courses_url_escaped'])
                page_driver.get(full_courses_url)
                time.sleep(5)
                element = WebDriverWait(page_driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div[aria-live="polite"] strong[aria-level="2"]')))
                raw_num_courses = element.text
                matches = re.findall(r'\d+', raw_num_courses)
                if matches:
                    num_courses = matches[-1]
                else:
                    num_courses = "No number of courses given"
            except (NoSuchElementException, TimeoutException):
                num_courses = "Failed to load courses page"


        courses_hyper_link = f'=HYPERLINK("{occ_info['courses_url_escaped']}", "{occ_info['courses_url_escaped']}")'

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
                             num_courses,
                             occ_info["vacancy_hyper_link"],
                             courses_hyper_link,
                             overview_interests_text,
                             overview_considerations_text,
                             dtd]
        append_row_with_retry(sheet, occupation_detail)
        time.sleep(3)

    page_driver.quit()
    print("Saved every data into the Google Sheet successfully.")

if __name__ == "__main__":
    main()

