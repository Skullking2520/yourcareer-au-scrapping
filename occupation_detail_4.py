# occupation_detail.py
import json
import re
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

def wait_for_page_load(wait_driver, timeout=15):
    try:
        WebDriverWait(wait_driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException:
        print("Page loading timeout.")
    except Exception as e:
        print(f"An error occurred while waiting for page load: {e}")

def remove_hyperlink(cell_value):
    # remove hyper link
    if cell_value.startswith('=HYPERLINK('):
        pattern = r'=HYPERLINK\("([^"]+)"\s*,\s*"[^"]+"\)'
        match = re.match(pattern, cell_value)
        if match:
            return match.group(1)
    return cell_value

def extract():
    # extract from Sheet1
    occ_sheet = web_sheet.get_worksheet("Occupation")
    occ_sheet_header = occ_sheet.row_values(1)
    try:
        link_idx = occ_sheet_header.index("occupation link") + 1
    except ValueError as e:
        print("Could not detect requested row", e)
        return

    all_rows = occ_sheet.get_all_values()[1:]
    link_list = []

    for row_num, row in enumerate(all_rows, start=2):
        link = row[link_idx - 1] if len(row) >= link_idx else ""
        detail_url = remove_hyperlink(link)
        if not detail_url:
            break
        link_list.append({"link_row_num":row_num, "detail_url":detail_url})
    return link_list

def overview_to_skills(link):
    # change overview tab url to skills tab url
    modified_link = re.sub(r"(\?|&)tab=overview", r"\1tab=skills", link)
    return modified_link

def batch_update_multiple_rows(worksheet, updates_list, retries=3, delay=10):
    sheet_id = getattr(worksheet, 'id', None) or worksheet._properties.get('sheetId')
    requests = []
    for row_num, updates in updates_list:
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
            if any(code in str(e) for code in ["500", "502", "503", "504"]):
                print(f"API Error ({e}). Retrying after {delay} seconds... (Attempt {attempt+1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    print("Failed to update cells after several attempts.")

def main():
    occ_sheet = web_sheet.get_worksheet("Occupation")
    progress_sheet = web_sheet.get_worksheet("Progress")
    extracted_list = extract()
    ph = ProcessHandler(progress_sheet, {"progress": "setting", "RowNum": 3}, "D2")
    progress = ph.load_progress()
    occ_sheet_header = occ_sheet.row_values(1)
    try:
        col_description = occ_sheet_header.index("description") + 1
        col_average_salary = occ_sheet_header.index("average salary") + 1
        col_future_demand = occ_sheet_header.index("future demand") + 1
        col_job_type = occ_sheet_header.index("job type") + 1
        col_skill_level = occ_sheet_header.index("skill level") + 1
        col_industry = occ_sheet_header.index("industry") + 1
        col_skills = occ_sheet_header.index("skills") + 1
        col_aat = occ_sheet_header.index("apprenticeships and traineeships") + 1
        col_overview_interests = occ_sheet_header.index("overview : interests") + 1
        col_overview_considerations = occ_sheet_header.index("overview : considerations") + 1
        col_overview_dtd = occ_sheet_header.index("overview : day-to-day") + 1

    except ValueError:
        print("Column not in sheet")
        return
    pending_updates = []
    while not progress["progress"] == "finished":
        try:
            progress["progress"] = "processing"
            while progress["RowNum"] < len(extracted_list):
                row_and_index = extracted_list[progress["RowNum"]]
                row_num = row_and_index["link_row_num"]
                extracted_url = row_and_index["detail_url"]
                url = remove_hyperlink(extracted_url)
                if extracted_url == "No detail url given":
                    description = "Failed to find description"
                    occupation_name = "Failed to find name"
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
                    progress["RowNum"] += 5
                else:
                    driver.get(url)
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    wait_for_page_load(driver)
                    print(f"current page: {url}")

                    # find description
                    try:
                        description = WebDriverWait(driver, 15).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div[class='text-lg']"))
                        ).text
                    except (NoSuchElementException,TimeoutException):
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
                        overview_interests = interests.find_elements(By.CSS_SELECTOR,
                                                                     "span[class='mint-pill__content-label']")
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
                            skills_elements = skills.find_elements(By.CSS_SELECTOR,
                                                                   "span[class='mint-pill__content-label']")
                            skills_list = []
                            for skills_element in skills_elements:
                                skills_list.append(skills_element.get_attribute("textContent"))
                            skills_text = ", ".join(skills_list)
                        except TimeoutException:
                            print("Skills section did not load in time.")
                            skills_text = "No skills given"
                        except Exception:
                            skills_text = "No skills given"
                    except (NoSuchElementException, TimeoutException) as e:
                        print(f"Error loading skills tab: {e}")
                        skills_text = "Failed to load skills page"

                updates = [(col_description, description),
                                     (col_average_salary, average_salary),
                                     (col_future_demand, future_demand),
                                     (col_job_type, job_type),
                                     (col_skill_level, skill_level),
                                     (col_industry, industry),
                                     (col_skills, skills_text),
                                     (col_aat, aat),
                                     (col_overview_interests, overview_interests_text),
                                     (col_overview_considerations, overview_considerations_text),
                                     (col_overview_dtd, dtd)]
                pending_updates.append((row_num, updates))
                time.sleep(3)
                progress["RowNum"] += 5
                if len(pending_updates) >= 20:
                    batch_update_multiple_rows(occ_sheet, pending_updates)
                    pending_updates = []

            if pending_updates:
                batch_update_multiple_rows(occ_sheet, pending_updates)
                pending_updates = []
            progress["progress"] = "finished"
            ph.save_progress(progress)
        except NoSuchElementException as e:
            print(f"Error processing detail: {e}")
            continue
    driver.quit()
    print("Saved every data into the Google Sheet successfully.")

if __name__ == "__main__":
    main()
