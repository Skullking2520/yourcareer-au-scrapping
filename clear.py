import json

from google_form_package import Sheet


def main():
    web_sheet = Sheet()
    occ_sheet = web_sheet.get_worksheet("Occupation")
    va_sheet = web_sheet.get_worksheet("Vacancies")
    progress_sheet = web_sheet.get_worksheet("Progress")
    occ_sheet.update([["Scrapping Finished"]], "R1")
    va_sheet.update([["Scrapping Finished"]], "Q1")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "UrlNum": 1})]], range_name="A1")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 0})]], range_name="A2")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 1})]], range_name="B2")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 2})]], range_name="C2")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 3})]], range_name="D2")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 4})]], range_name="E2")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "UrlNum": 1})]], range_name="A3")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 0})]], range_name="A4")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 1})]], range_name="B4")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 2})]], range_name="C4")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 3})]], range_name="D4")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 4})]], range_name="E4")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 5})]], range_name="F4")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 6})]], range_name="G4")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 7})]], range_name="H4")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 0})]], range_name="A5")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 1})]], range_name="B5")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 2})]], range_name="C5")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 3})]], range_name="D5")
    progress_sheet.update(values=[[json.dumps({"progress": "setting", "RowNum": 4})]], range_name="E5")
if __name__ == "__main__":
    main()
