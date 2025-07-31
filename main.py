import gspread
import time
from collections import defaultdict

gc = gspread.service_account("./creds.json")

# Open a sheet from a spreadsheet in one go
sheet = gc.open("order").sheet1

# records = sheet.get_all_records()
# gclid_column = sheet.col_values(sheet.find("gclid").col)  # знаходимо колонку за назвою
# utm_source_column = sheet.col_values(sheet.find("utm_source").col)
#
# utm_source_count = len([x for x in utm_source_column if x.strip() != ""]) - 1
# utm_source_value = {
#     "google": 0,
#     "facebook": 0
# }
# for x in utm_source_column:
#     if x.strip() != "":
#         if "google" in x.strip():
#             utm_source_value["google"] += 1
#         elif "facebook" in x.strip():
#             utm_source_value["facebook"] += 1
#         elif x.strip() not in utm_source_value:
#             utm_source_value[x.strip()] = 1
#         else:
#             utm_source_value[x.strip()] += 1
# gclid_count = len([x for x in gclid_column if x.strip() != ""]) - 1  # -1, щоб виключити заголовок
# num_rows = len(records)
#
# print(f"Кількість заповнених рядків (без заголовка): {num_rows}")
# print(f"Кількість заповнених рядків у колонці 'gclid': {gclid_count}")
# print(f"Кількість заповнених рядків у колонці 'utm_source': {utm_source_count}")
# print(utm_source_value)
# Отримуємо заголовки
headers = sheet.row_values(1)

# Додаємо колонку для результатів (якщо потрібно)
if "Джерело трафіку" not in headers:
    sheet.insert_cols([["Джерело трафіку"]], len(headers) + 1)
    headers = sheet.row_values(1)  # Оновлюємо заголовки

# Функція класифікації
def classify_traffic(row_data):
    gclid = row_data.get('gclid', '')
    fbclid = row_data.get('fbclid', '')
    utm_source = str(row_data.get('utm_source', '')).lower()
    utm_medium = str(row_data.get('utm_medium', '')).lower()
    
    if gclid and str(gclid).strip() not in ('', 'nan'):
        return "Google Ads"
    elif "google" in utm_source and not (gclid and str(gclid).strip() not in ('', 'nan')):
        return "Google Органіка"
    elif fbclid and str(fbclid).strip() not in ('', 'nan'):
        return "Meta Ads"
    elif any(x in utm_source for x in ["fb", "facebook"]) and not (fbclid and str(fbclid).strip() not in ('', 'nan')):
        return "Meta Органіка"
    elif any(x in utm_medium for x in ["email", "mail", "newsletter"]):
        return "Email-маркетинг"
    elif not any([row_data.get(h, '') for h in headers if h not in ['gclid', 'fbclid']]):
        return "Direct"
    else:
        return "Інші"

# Налаштування батчів
BATCH_SIZE = 1000
SLEEP_TIME = 60

# Лічильники категорій
category_counts = defaultdict(int)

# Отримуємо загальну кількість рядків
total_rows = len(sheet.col_values(1))

print(f"Початок обробки {total_rows} рядків...")

# Основний цикл обробки
for start_row in range(2, total_rows + 1, BATCH_SIZE):
    end_row = min(start_row + BATCH_SIZE - 1, total_rows)
    
    print(f"Обробка рядків {start_row}-{end_row}...")
    
    # Отримуємо дані батча
    batch_range = f"A{start_row}:{chr(64 + len(headers))}{end_row}"
    rows_data = sheet.get(batch_range)
    
    # Готуємо дані для оновлення
    updates = []
    for i, row in enumerate(rows_data, start=start_row):
        row_dict = dict(zip(headers, row))
        source = classify_traffic(row_dict)
        updates.append([source])
        category_counts[source] += 1  # Лічимо категорії
    
    # Записуємо батч
    result_range = f"{chr(64 + len(headers))}{start_row}:{chr(64 + len(headers))}{end_row}"
    sheet.update(result_range, updates)
    
    # Пауза для обходу лімітів API
    if end_row < total_rows:
        print(f"Очікування {SLEEP_TIME} секунд перед наступним батчем...")
        time.sleep(SLEEP_TIME)

# Виводимо статистику
print("\nРезультати підрахунку:")
for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"{category}: {count} ({count/total_rows*100:.1f}%)")

# Записуємо статистику в новий аркуш (опційно)
try:
    stats_sheet = gc.open("order").add_worksheet(title="Статистика", rows=100, cols=2)
    stats_data = [["Категорія", "Кількість"]] + [[k, v] for k, v in category_counts.items()]
    stats_sheet.update("A1", stats_data)
    print("\nСтатистику записано на новий аркуш 'Статистика'")
except Exception as e:
    print(f"\nНе вдалося створити аркуш статистики: {e}")

print("\nОбробку завершено!")
