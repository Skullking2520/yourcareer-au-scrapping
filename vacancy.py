from urllib.parse import urljoin #join url
from selenium import webdriver # web scrapping
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from google.oauth2.service_account import Credentials # google doc
import time
import os
import re
import gspread
import datetime

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

def set_vacancy_sheet():
    # set for vacancy sheet
    worksheet = get_worksheet("Vacancies")
    worksheet.clear()
    headers = ["occupation","date added", "time scrapped", "job title", "job link", "job code", "company", "salary", "address", "lat", "long", "tenure","overview", "closes","description"]
    worksheet.append_row(headers)
    return worksheet

def append_row_with_retry(worksheet, data, retries=3, delay=5):
    for attempt in range(retries):
        try:
            worksheet.append_row(data, value_input_option="USER_ENTERED")
            return
        except gspread.exceptions.APIError as e:
            if "503" in str(e):
                print(f"Error 503 occurred. Retry after {delay}seconds ({attempt+1}/{retries})")
                time.sleep(delay)
            else:
                raise

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

def check_extract():
    va_sheet = get_worksheet("Vacancies")
    va_header = va_sheet.row_values(1)

    try:
        va_title = va_header.index("job title") + 1
        va_company = va_header.index("company") + 1
    except ValueError as e:
        print("Could not detect requested row", e)
        return

    all_rows = va_sheet.get_all_values()[1:]

    check_list = []

    for row_num, row in enumerate(all_rows, start=2):
        title = row[va_title - 1] if len(row) >= va_title else ""
        company = row[va_company - 1] if len(row) >= va_company else ""
        check_list.append([title, company])
    return check_list


def update_occupation_cell(job_title, company, va_occupation):
    va_sheet = get_worksheet("Vacancies")
    va_header = va_sheet.row_values(1)

    try:
        job_title_index = va_header.index("job title") + 1
        company_index = va_header.index("company") + 1
        occupation_index = va_header.index("occupation") + 1
    except ValueError:
        return

    all_rows = va_sheet.get_all_values()[1:]

    for row_num, row in enumerate(all_rows, start=2):
        title = row[job_title_index - 1] if len(row) >= job_title_index else ""
        comp = row[company_index - 1] if len(row) >= company_index else ""

        if [job_title.lower(), company.lower()] == [title.lower(), comp.lower()]:
            current_value = va_sheet.cell(row_num, occupation_index).value

            if current_value:
                new_value = current_value + "," + str(va_occupation)
            else:
                new_value = str(va_occupation)

            va_sheet.update_cell(row_num, occupation_index, new_value)
            break

def remove_hyperlink(cell_value):
    # remove hyper link
    if cell_value.startswith('=HYPERLINK('):
        pattern = r'=HYPERLINK\("([^"]+)"\s*,\s*"[^"]+"\)'
        match = re.match(pattern, cell_value)
        if match:
            return match.group(1)
    return cell_value

def main():
    init = set_vacancy_sheet()
    va_sheet = get_worksheet("Vacancies")
    driver = set_driver()

    for url_data in extract():
        va_occupation = url_data[0]
        va_url = url_data[1]

        all_vacancy_data = []
        seen_jobs = set()
        check_list = check_extract()

        driver.get(va_url)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        
        while True:
            vacancies = driver.find_elements(By.CSS_SELECTOR,
                "section[class='mint-search-result-item has-img has-actions has-preheading']")
            time.sleep(3)

            for vacancy in vacancies:
                # find job title
                try:
                    base_url = "https://www.workforceaustralia.gov.au"
                    job_hyper = vacancy.find_element(By.CSS_SELECTOR,"a[class='mint-link link']")
                    job_title = job_hyper.text
                    job_href = job_hyper.get_attribute("href")
                    job_link = urljoin(base_url, job_href)
                    job_code = job_href.split('/')[-1]
                except NoSuchElementException:
                    job_title = "No job title given"
                    job_link = "No job link given"
                    job_code = "No job code given"

                try:
                    raw_date_added_dif = vacancy.find_element(By.CSS_SELECTOR,"div[class='preheading']").text
                    match = re.search(r'\d+', raw_date_added_dif)
                    if match:
                        date_added_dif = int(match.group())
                    else:
                        date_added_dif = 1
                    today = datetime.date.today()
                    date_added = today - datetime.timedelta(days=date_added_dif)
                except NoSuchElementException:
                    date_added = "No date added given"

                time_scrapped = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

                try:
                    overview = vacancy.find_element(By.CSS_SELECTOR,"span[class='mint-blurb__text-width']").text
                except NoSuchElementException:
                    overview = "No overview given"

                vacancy_data = {
                    "job_title": job_title,
                    "job_link": job_link,
                    "job_code": job_code,
                    "date_added": str(date_added),
                    "time_scrapped": str(time_scrapped),
                    "overview": overview
                }
                all_vacancy_data.append(vacancy_data)

            try:
                next_button = driver.find_element(By.CSS_SELECTOR, "button[aria-label='Go to next page']")
                driver.execute_script("arguments[0].click();", next_button)
                time.sleep(3)
            except NoSuchElementException:
                break

        for vac_element in all_vacancy_data:
            # open detail page
            driver.get(vac_element["job_link"])
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            print(f"current page: {vac_element['job_title']}")

            try:
                company = driver.find_element(By.XPATH, "//div[@class='text-lg' and @dir='auto']//p/a").text
            except NoSuchElementException:
                company = "No company given"

            if (vac_element['job_title'].lower(), company.lower()) in seen_jobs:
                print(f"Duplicate found, skipping: {company}, {vac_element['job_title']}")
                continue

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
                closes = driver.find_element(By.CSS_SELECTOR, "ul.job-info-metadata > li:nth-child(4) > span:nth-child(2)").text
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
                time.sleep(10)
                map_url = driver.current_url
                pattern = r"@(-?\d+\.\d+),(-?\d+\.\d+)"
                match = re.search(pattern, map_url)
                if match:
                    va_lat, va_long = match.groups()
                else:
                    va_lat = "No lat given"
                    va_long = "No long given"
            except NoSuchElementException:
                va_lat = "No lat given"
                va_long = "No long given"

            va_job_link = f'=HYPERLINK("{vac_element['job_link']}", "{vac_element['job_link']}")'

            if [vac_element['job_title'].lower(), company.lower()] in check_list:
                update_occupation_cell(vac_element['job_title'], company, va_occupation)
            else:
                va_data = [
                    va_occupation,
                    vac_element["date_added"],
                    vac_element["time_scrapped"],
                    vac_element['job_title'],
                    va_job_link,
                    vac_element['job_code'],
                    company,
                    salary,
                    address,
                    va_lat,
                    va_long,
                    tenure,
                    vac_element['overview'],
                    closes,
                    job_description
                ]
                append_row_with_retry(va_sheet,va_data)
                seen_jobs.add((vac_element['job_title'].lower(), company.lower()))

    driver.quit()
    print("Saved every data into the Google Sheet successfully.")

if __name__ == "__main__":
    main()
