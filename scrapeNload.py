import re
import sys
import json
import time
import random
import atexit
import gspread  # Google Sheets API
import threading
from dateutil import parser
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

def main():
    # Configuration Starts here

    # ✅ Google Sheets API Configuration
    SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    CREDENTIALS_FILE = "creds.json"  # Ensure this file is present on the new machine

    # Authenticate with Google Sheets
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPE)
        client = gspread.authorize(creds)
        print("✅ Successfully authenticated with Google Sheets API.")
    except Exception as e:
        print(f"❌ Error: Could not authenticate with Google Sheets API. Ensure 'credentials.json' is correct.")
        sys.exit(1)

    # ✅ Load JSON Configuration
    CONFIG_FILE = "config.json"

    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as file:
            config = json.load(file)
        print("✅ Successfully loaded JSON configuration.")
    except Exception as e:
        print(f"❌ Error: Could not read config.json - {e}")
        sys.exit(1)

    #Configure Chrome options
    options = Options()

    # ✅ Function to format and map data
    def format_row(row):
        # Extract relevant fields dynamically (handles variations in table structure)
        date_raw = row[0]  # Date (e.g., "07.02.2025 (Gen)")
        actual = row[2]  # Actual value (e.g., "143K")
        forecast = row[3]  # Forecast value (e.g., "169K")
        p = row[len(row) - 1] # Preliminary indicator

        # Convert "143K" → "143,000"
        def convert_k_to_number(value):
            value = value.replace("K", "000") if "K" in value else value
            value = value.replace(".", ",") if "." in value else value
            return value  # If not a number, return as is

        def normalize_date(date_str):
            """
            Converts various date formats into a standard DD/MM/YYYY format.
            - Handles cases with unwanted text (e.g., "07.02.2025 (Gen)")
            - Works with European (DD.MM.YYYY), American (Feb 7, 2025), ISO (2025-02-07), and shorthand (07-02-25).
            - Ensures always returning a clean date.
            """
            try:
                # ✅ Step 1: Remove any text inside parentheses (e.g., "(Gen)", "(Mar)", etc.)
                cleaned_date = re.sub(r"\(.*?\)", "", date_str).strip()

                # ✅ Step 2: Remove extra spaces and unnecessary words
                cleaned_date = re.sub(r"[^0-9A-Za-z./\- ]", "", cleaned_date).strip()

                # ✅ Step 3: Automatically detect and parse the date
                parsed_date = parser.parse(cleaned_date, dayfirst=True)  # Always prefer European format (DD/MM/YYYY)

                # ✅ Step 4: Convert to DD/MM/YYYY format
                return parsed_date.strftime("%d/%m/%Y")

            except Exception as e:
                print(f"❌ Date parsing failed for '{date_str}': {e}")
                return date_str  # Return the original string if parsing fails

        formatted_date = normalize_date(date_raw)
        formatted_actual = convert_k_to_number(actual)
        formatted_forecast = convert_k_to_number(forecast)

        # Return values in the correct order: Date, Forecast, Actual
        return [formatted_date, formatted_forecast, formatted_actual, True if p == "p" else False]

    def normalize_sheet_value(value):
        """
            Normalizes Google Sheets number format:
            - Converts '133.000' → '133000'
            - Converts '2,50%' → '2,5%'
            """
        value = value.strip()

        # ✅ Normalize percentages like "2,50%" → "2,5%"
        if value.endswith("%"):
            number_part = value[:-1].replace(",", ".")  # Use . for float parsing
            try:
                normalized_float = float(number_part)
                # Remove trailing zeroes and convert back to comma format
                cleaned = f"{normalized_float}".replace(".", ",").rstrip(",")
                return f"{cleaned}%"
            except ValueError:
                print("Error during percentage normalization! Terminating...")
                sys.exit(1)

        # ✅ Normalize regular numbers like '133.000' → '133000'
        elif "." in value:
            return value.replace(".", "")

        return value

    def update_config_file(config, config_file_path, spreadsheet_url, tab_name, scrape_url, new_row):
        """
        ✅ Updates the JSON config file to set the new row number after successful data fetch.
        """
        for sheet in config["spreadsheets"]:
            if sheet["url"] == spreadsheet_url:
                for tab, scraping_tasks in sheet["tabs"].items():
                    if tab == tab_name:
                        for task in scraping_tasks:
                            if task["url"] == scrape_url:
                                print(f"🔄 Updating row in config: {task['row']} → {new_row}")
                                task["row"] = new_row  # Update row
                                break

        # ✅ Save the updated configuration
        try:
            with open(config_file_path, "w", encoding="utf-8") as file:
                json.dump(config, file, indent=4)
            print("✅ Config file updated successfully.")
        except Exception as e:
            print(f"❌ Failed to update config.json: {e}")

    def compare_dates(new_date, sheet_date):
        """
        ✅ Compares two dates and returns:
        - 0 if they are the same
        - 1 if the new date is later (newer data)
        - -1 if the new date is earlier (should not happen)
        """
        try:
            new_dt = datetime.strptime(new_date, "%d/%m/%Y")
            sheet_dt = datetime.strptime(sheet_date, "%d/%m/%Y")

            print(new_dt, sheet_dt)

            if new_dt > sheet_dt:
                return 1  # ✅ Newer date → Move to next row
            elif new_dt == sheet_dt:
                return 0  # 🚫 Duplicate → Do not increment row
            else:
                return -1  # ❌ Something is wrong (new data should not be older)
        except Exception as e:
            print(f"⚠️ Error comparing dates: {e}")
            return -1  # Default to not increment if comparison fails

    # Logic Starts Here

    # ✅ Process Each Google Spreadsheet from JSON
    for sheet_config in config["spreadsheets"]:
        spreadsheet_url = sheet_config["url"]

        try:
            spreadsheet = client.open_by_url(spreadsheet_url)
            print(f"\n🔹 Processing Spreadsheet: {spreadsheet.title}")
        except Exception as e:
            print(f"❌ Error: Could not access Google Spreadsheet {spreadsheet_url}. Skipping... {e}")
            continue

        # ✅ Loop Through Tabs in Each Spreadsheet
        for tab_name, scraping_tasks in sheet_config["tabs"].items():
            print(f"\n🔹 Processing Tab: {tab_name}")

            try:
                sheet = spreadsheet.worksheet(tab_name)
                print(f"  ✅ Opened tab: {tab_name}")
            except Exception as e:
                print(f"  ❌ Error: Could not find tab '{tab_name}' in Google Sheets. Skipping...")
                continue

            # ✅ Loop Through Each Scraping Task for This Tab
            for task in scraping_tasks:
                url = task["url"]
                start_row = task["row"]
                start_col = task["column"]
                fields = task["fields"]

                try:
                    with (webdriver.Chrome(options=options) as driver):
                        driver.get(url)
                        time.sleep(2) # Time to let the page load properly
                        print(f"\n🔹 Scraping: {url} (Row: {start_row}, Column: {start_col})")

                        try:
                            # Click on "Cerca" button
                            table = driver.find_element(By.TAG_NAME, "table")
                            thead = table.find_element(By.TAG_NAME, "thead")
                            headers = [th.text.strip() for th in thead.find_elements(By.TAG_NAME, "th")]
                            tbody = table.find_element(By.TAG_NAME, "tbody")
                            rows = tbody.find_elements(By.TAG_NAME, "tr")
                            table_data = []

                            # ✅ Fetch first 4 rows if size allows it.
                            if isinstance(start_row, list) and isinstance(start_col, list) and len(rows) > 3:
                                for row in rows[:4]:
                                    cols = row.find_elements(By.TAG_NAME, "td")
                                    row_data = [col.text.strip() for col in cols]  # Clean the text

                                    # ✅ Check for the "Preliminary" span inside each cell
                                    p_indicator = any(
                                        'smallGrayP' in col.get_attribute("innerHTML")
                                        for col in cols
                                    )

                                    if row_data:
                                        if p_indicator:
                                            row_data.append("p")  # 👈 or any flag you prefer
                                        else:
                                            row_data.append("")  # To keep consistent column length
                                        table_data.append(row_data)
                            elif isinstance(start_row, list) and isinstance(start_col, list):
                                for row in rows[:2]:
                                    cols = row.find_elements(By.TAG_NAME, "td")
                                    row_data = [col.text.strip() for col in cols]  # Clean the text

                                    # ✅ Check for the "Preliminary" span inside each cell
                                    p_indicator = any(
                                        'smallGrayP' in col.get_attribute("innerHTML")
                                        for col in cols
                                    )

                                    if row_data:
                                        if p_indicator:
                                            row_data.append("p")  # 👈 or any flag you prefer
                                        else:
                                            row_data.append("")  # To keep consistent column length
                                        table_data.append(row_data)
                            else:
                                for row in rows[:2]:
                                    cols = row.find_elements(By.TAG_NAME, "td")
                                    row_data = [col.text.strip() for col in cols]  # Clean the text
                                    if row_data:
                                        table_data.append(row_data)

                            # Ensure at least 2 rows were extracted
                            if len(table_data) < 2 or any(len(row) == 0 for row in table_data):
                                print("❌ Not enough valid rows found in the table. Exiting.")
                                sys.exit(1)

                            # ✅ Apply formatting to table_data
                            table_data = list(map(format_row, table_data))

                            # ✅ Get existing Google Sheet data
                            existing_data = sheet.get_all_values()
                            last_sheet_row = len(existing_data)  # Last row index in Google Sheets (1-based)

                            if not isinstance(start_row, list):
                                # ✅ Extract the first scraped row (most recent)
                                first_scraped_row = table_data[0][:3]

                                if fields == ["Date", "Actual"]:
                                    first_scraped_row = [first_scraped_row[0], first_scraped_row[2]]
                                    # ✅ Read the current data in the specified row to check if an update is needed
                                    existing_first_row_data = existing_data[start_row - 1][start_col - 1:start_col + 1] if len(
                                        existing_data) >= start_row else ["", ""]
                                    print(first_scraped_row, existing_first_row_data)
                                else:
                                    # ✅ Read the current data in the specified row to check if an update is needed
                                    existing_first_row_data = existing_data[start_row - 1][start_col - 1:4] if len(
                                        existing_data) >= start_row else ["", "", ""]
                                    print(first_scraped_row, existing_first_row_data)

                                existing_first_row_data = [normalize_sheet_value(val) for val in existing_first_row_data]

                                # ✅ Compare new date with existing date in the sheet
                                date_comparison = compare_dates(first_scraped_row[0], existing_first_row_data[0])
                                prev_row_index = start_row - 1

                                # ✅ Case 1: Newer date → Update `config.json` first, then write data
                                if date_comparison == 1:
                                    print(f"🔄 Newer data detected. Updating config.json first.")

                                    # ✅ Update `config.json` row pointer **before** writing to the sheet
                                    new_row = start_row + 1
                                    prev_row_index += 1

                                    update_config_file(config, CONFIG_FILE, spreadsheet_url, tab_name, url, new_row)
                                    print(f"✅ Row updated in config.json: Next scrape will start from row {new_row}")

                                    # ✅ Now write the new data to the sheet
                                    for col_index, cell_value in enumerate(first_scraped_row):
                                        sheet.update_cell(new_row, start_col + col_index, cell_value)
                                    print(f"✅ First fetched row ({first_scraped_row[0]}) inserted at row {new_row}.")

                                # ✅ Case 2: Same date → Check if values differ, update if necessary (but don't increment row)
                                elif date_comparison == 0:
                                    print(f"🔄 Same date detected. Checking if update is needed...")

                                    if first_scraped_row != existing_first_row_data:
                                        for col_index, cell_value in enumerate(first_scraped_row):
                                            sheet.update_cell(start_row, start_col + col_index, cell_value)
                                        print(f"✅ Data updated at row {start_row} (without moving row pointer).")
                                    else:
                                        print(f"🚫 No changes needed: Data is identical at row {start_row}.")

                                # ✅ Case 3: Fetched date is somehow older (should not happen)
                                else:
                                    print(f"⚠️ Warning: Fetched date is earlier than existing date. Skipping update.")
                                    continue

                                if fields == ["Date", "Actual"]:
                                    second_scraped_row = [table_data[1][0], table_data[1][2]]
                                    # ✅ Read the current data in the specified row to check if an update is needed
                                    existing_prev_row_data = existing_data[prev_row_index - 1][start_col - 1:start_col + 1] if len(
                                        existing_data) >= prev_row_index else ["", ""]
                                else:
                                    second_scraped_row = table_data[1][:3]
                                    existing_prev_row_data = existing_data[prev_row_index - 1][start_col - 1:4] if len(
                                        existing_data) >= prev_row_index else ["", "", ""]

                                existing_prev_row_data = [normalize_sheet_value(val) for val in existing_prev_row_data]
                                # ✅ Overwrite the previous row **only if there are changes**
                                if second_scraped_row != existing_prev_row_data:
                                    for col_index, cell_value in enumerate(second_scraped_row):
                                        sheet.update_cell(prev_row_index, start_col + col_index, cell_value)
                                    print(
                                        f"✅ Overwritten previous row {prev_row_index} with new data ({second_scraped_row[0]}).")
                                else:
                                    print(f"🚫 No changes needed for previous row {prev_row_index}, data already matches.")
                            else:
                                # Gets the highest index for row-checking purposes
                                max_row_index = max(start_row)

                                # ✅ First two are current values, last two are confirmations
                                for i, row_data in enumerate(table_data):
                                    # Values + P-flag set up
                                    values = [row_data[0], row_data[2]]
                                    p_flag = row_data[3]

                                    # Targets the proper sheet indices by addressing p-flag
                                    row_col_indices = [0, 1] if not p_flag else [2]

                                    if(i < 2):
                                        for j in row_col_indices:
                                            existing_values = existing_data[start_row[j] - 1][start_col[j] - 1:start_col[j] + 1] if len(
                                            existing_data) >= max_row_index else ["", ""]
                                            existing_values = [normalize_sheet_value(val) for val in existing_values]
                                            print(values, existing_values)

                                            # ✅ Compare new date with existing date in the sheet
                                            date_comparison = compare_dates(values[0],
                                                                            existing_values[0])

                                            # ✅ Case 1: Newer date → Update `config.json` first, then write data
                                            if date_comparison == 1:
                                                print(f"🔄 Newer data detected. Updating config.json first.")

                                                # ✅ Update `config.json` row pointer **before** writing to the sheet
                                                new_row = start_row.copy()
                                                new_row[j] += 1
                                                # Normalize again max index for safety
                                                max_row_index = max(new_row)

                                                update_config_file(config, CONFIG_FILE, spreadsheet_url, tab_name,
                                                                   url, new_row)
                                                print(
                                                    f"✅ Row updated in config.json: Next scrape will start from row {new_row}")

                                                # ✅ Now write the new data to the sheet
                                                for col_index, cell_value in enumerate(values):
                                                    sheet.update_cell(new_row[j], start_col[j] + col_index, cell_value)
                                                print(
                                                    f"✅ First fetched row ({values[0]}) inserted at row {new_row}.")

                                            # ✅ Case 2: Same date → Check if values differ, update if necessary (but don't increment row)
                                            elif date_comparison == 0:
                                                print(f"🔄 Same date detected. Checking if update is needed...")

                                                if values != existing_values:
                                                    for col_index, cell_value in enumerate(values):
                                                        sheet.update_cell(start_row[j], start_col[j] + col_index,
                                                                          cell_value)
                                                    print(
                                                        f"✅ Data updated at row {start_row[j]} (without moving row pointer).")
                                                else:
                                                    print(
                                                        f"🚫 No changes needed: Data is identical at row {start_row[j]}.")

                                            # ✅ Case 3: Fetched date is somehow older (should not happen)
                                            else:
                                                print(
                                                    f"⚠️ Warning: Fetched date is earlier than existing date. Skipping update.")
                                                continue
                                    else: # Confirmation of the previous values if present
                                        for j in row_col_indices:
                                            existing_prev_values = existing_data[start_row[j] - 2][
                                                              start_col[j] - 1:start_col[j] + 1] if len(
                                                existing_data) >= max_row_index - 1 else ["", ""]
                                            existing_prev_values = [normalize_sheet_value(val) for val in
                                                               existing_prev_values]
                                            print(values, existing_prev_values)

                                            # ✅ Overwrite the previous row **only if there are changes**
                                            if values != existing_prev_values:
                                                for col_index, cell_value in enumerate(values):
                                                    sheet.update_cell(start_row[j]-1, start_col[j] + col_index, cell_value)
                                                print(
                                                    f"✅ Overwritten previous row {start_row[j]-1} with new data ({values[0]}).")
                                            else:
                                                print(
                                                    f"🚫 No changes needed for previous row {start_row[j]-1}, data already matches.")
                        except Exception as error:
                            print(error)
                            print("Error! Skipping...")
                            continue
                except Exception as error:
                    print("Something went wrong opening " + url + ", skipping...")
                    continue
                finally:
                    print("Quitting driver....")

        print("\nDone. Moving to the next sheet...")

    print("\n🔻 Scraping process completed.")

def on_exit():
    print("🚪 Program is exiting...")

atexit.register(on_exit)

if __name__ == '__main__':
    main()
    print("Active threads:", threading.enumerate())
    sys.exit(0)
