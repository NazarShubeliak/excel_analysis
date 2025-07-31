import gspread
import time
from collections import defaultdict

BATCH_SIZE = 1000
SLEEP_TIME = 60
TOKEN = "./creds.json"
TABLE_NAME = "order"

# Classification function
def classify_traffic(row_data):
    gclid = row_data.get('gclid', '')
    fbclid = row_data.get('fbclid', '')
    utm_source = str(row_data.get('utm_source', '')).lower()
    utm_medium = str(row_data.get('utm_medium', '')).lower()
    
    if gclid and str(gclid).strip() not in ('', 'nan'):
        return "Google Ads"
    elif "google" in utm_source and not (gclid and str(gclid).strip() not in ('', 'nan')):
        return "Google Organic"
    elif fbclid and str(fbclid).strip() not in ('', 'nan'):
        return "Meta Ads"
    elif any(x in utm_source for x in ["fb", "facebook"]) and not (fbclid and str(fbclid).strip() not in ('', 'nan')):
        return "Meta Organic"
    elif any(x in utm_medium for x in ["email", "mail", "newsletter"]):
        return "Email"
    elif not any([row_data.get(h, '') for h in headers if h not in ['gclid', 'fbclid']]):
        return "Direct"
    else:
        return "Others"


gc = gspread.service_account(TOKEN)

# Open a sheet from a spreadsheet in one go
sheet = gc.open(TABLE_NAME).sheet1
headers = sheet.row_values(1)

# Add a column for results (if necessary)
if "Джерело трафіку" not in headers:
    sheet.insert_cols([["Джерело трафіку"]], len(headers) + 1)
    headers = sheet.row_values(1)  # Updating the headlines


# Category counters
category_counts = defaultdict(int)

# We get the total number of rows
total_rows = len(sheet.col_values(1))

print(f"Starting processing {total_rows} rows...")

# The main cycle
for start_row in range(2, total_rows + 1, BATCH_SIZE):
    end_row = min(start_row + BATCH_SIZE - 1, total_rows)
    
    print(f"String processing {start_row}-{end_row}...")
    
    # Getting batch data 
    batch_range = f"A{start_row}:{chr(64 + len(headers))}{end_row}"
    rows_data = sheet.get(batch_range)
    
    # Preparing data for update
    updates = []
    for i, row in enumerate(rows_data, start=start_row):
        row_dict = dict(zip(headers, row))
        source = classify_traffic(row_dict)
        updates.append([source])
        category_counts[source] += 1  # Count category
    
    # Recording a batch
    result_range = f"{chr(64 + len(headers))}{start_row}:{chr(64 + len(headers))}{end_row}"
    sheet.update(result_range, updates)
    
    # Pause to bypass API limits
    if end_row < total_rows:
        print(f"Waiting {SLEEP_TIME} seconds before next batch...")
        time.sleep(SLEEP_TIME)

# Displaying statistics
print("\nCounting results:")
for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"{category}: {count} ({count/total_rows*100:.1f}%)")

# Write statistics to a new sheet
try:
    stats_sheet = gc.open("order").add_worksheet(title="Statistic", rows=100, cols=2)
    stats_data = [["Category", "Count"]] + [[k, v] for k, v in category_counts.items()]
    stats_sheet.update("A1", stats_data)
    print("\nStatistics are written to a new sheet 'Statistics'")
except Exception as e:
    print(f"\nFailed to create statistics sheet: {e}")

print("\nProcessing completed!")
