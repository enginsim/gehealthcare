import gspread
import csv

SERVICE_ACCOUNT_FILE = 'tokyo-scholar-464119-b4-6a8fa808f85e.json' 
SPREADSHEET_NAME = 'My Data Sheet' 
WORKSHEET_NAME = 'Sheet1' 
CSV_FILE_PATH = 'data/processed/allianz_2025Q1.csv' 

def write_csv_to_google_sheet(service_account_file, spreadsheet_name, worksheet_name, csv_file_path):
    try:
       
        gc = gspread.service_account(filename=service_account_file)
        spreadsheet = gc.open(spreadsheet_name)
        print(f"Opened spreadsheet: '{spreadsheet_name}'")

        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet '{worksheet_name}' not found.")
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="100", cols="20")

        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            data = list(reader)

        worksheet.clear() 
        worksheet.update('A1', data)
        print("Success")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    write_csv_to_google_sheet(SERVICE_ACCOUNT_FILE, SPREADSHEET_NAME, WORKSHEET_NAME, CSV_FILE_PATH)