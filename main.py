import os
import logging
from collections import Counter
from datetime import datetime, timedelta
import requests
import ast
import psycopg2

import gspread
from google.oauth2.service_account import Credentials

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

# Удаляем логи старше 3 дней
for filename in os.listdir(log_dir):
    if filename.endswith(".log"):
        try:
            file_date = datetime.strptime(filename.replace(".log", ""), "%Y-%m-%d")
            if datetime.now() - file_date > timedelta(days=3):
                os.remove(os.path.join(log_dir, filename))
        except Exception as e:
            print(f"Не удалось обработать лог-файл {filename}: {e}")

# Настройка логирования
log_file = os.path.join(log_dir, f"{datetime.now():%Y-%m-%d}.log")
logging.basicConfig(
    filename=log_file,
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)


api_url = "https://b2b.itresume.ru/api/statistics"
params = {
    "client": "Skillfactory",
    "client_key": "M2MGWS",
    "start": "2025-12-31 00:00:00",
    "end": "2026-01-10 23:59:59"
}

logging.info("Начинаем загрузку данных с API")
try:
    response = requests.get(api_url, params=params)
    response.raise_for_status()
    data = response.json()
    logging.info(f"Данные успешно получены, записей: {len(data)}")
except requests.RequestException as e:
    logging.error(f"Ошибка при запросе к API: {e}")
    data = []


processed_data = []

for item in data:
    raw_passback = item.get("passback_params")
    if not raw_passback or not isinstance(raw_passback, str) or raw_passback.strip() == "":
        logging.error(f"passback_params отсутствует или не строка, запись пропущена (user_id={item.get('lti_user_id')})")
        continue
    try:
        passback = ast.literal_eval(raw_passback)
        # is_correct
        raw_is_correct = item.get("is_correct")
        if raw_is_correct is None:
            is_correct = None
        elif raw_is_correct in (1, True):
            is_correct = True
        elif raw_is_correct in (0, False):
            is_correct = False
        else:
            logging.error(f"Некорректное is_correct: {raw_is_correct}")
            continue

        processed_data.append({
            "user_id": item.get("lti_user_id"),
            "oauth_consumer_key": passback.get("oauth_consumer_key"),
            "lis_result_sourcedid": passback.get("lis_result_sourcedid"),
            "lis_outcome_service_url": passback.get("lis_outcome_service_url"),
            "is_correct": is_correct,
            "attempt_type": item.get("attempt_type"),
            "created_at": item.get("created_at")
        })

    except (ValueError, SyntaxError) as e:
        logging.error(f"Некорректный passback_params: {e}, запись пропущена")
        continue
    except Exception as e:
        logging.error(f"Ошибка при обработке записи: {e}")

logging.info(f"Предобработка данных завершена, подготовлено {len(processed_data)} записей")


try:
    conn = psycopg2.connect(
        dbname="simulative",
        user="simulative",
        password="simulative",
        host="localhost",
        port="5434"
    )
    cursor = conn.cursor()
    logging.info("Подключение к базе данных установлено")

    # Создание таблицы, если её нет
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS grader_attempts (
        user_id TEXT,
        oauth_consumer_key TEXT,
        lis_result_sourcedid TEXT,
        lis_outcome_service_url TEXT,
        is_correct BOOLEAN,
        attempt_type TEXT,
        created_at TIMESTAMP
    )
    """)
    conn.commit()
    logging.info("Таблица grader_attempts готова")

    # Вставка данных
    logging.info("Начинаем загрузку данных в базу")
    for item in processed_data:
        try:
            cursor.execute("""
            INSERT INTO grader_attempts (
                user_id, oauth_consumer_key, lis_result_sourcedid,
                lis_outcome_service_url, is_correct, attempt_type, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                item["user_id"], item["oauth_consumer_key"], item["lis_result_sourcedid"],
                item["lis_outcome_service_url"], item["is_correct"], item["attempt_type"], item["created_at"]
            ))
        except Exception as e:
            logging.error(f"Ошибка при вставке записи: {e}, запись пропущена")

    conn.commit()
    logging.info("Данные успешно загружены в базу")

except Exception as e:
    logging.error(f"Ошибка работы с базой данных: {e}")

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    logging.info("Соединение с базой данных закрыто")


total_attempts = len(processed_data)
successful_attempts = sum(1 for x in processed_data if x["is_correct"] is True)
unique_users = len(set(x["user_id"] for x in processed_data))
attempts_per_type = Counter(x["attempt_type"] for x in processed_data)
attempts_per_user_count = len(set(x["user_id"] for x in processed_data))

daily_stats = {
    "date": datetime.now().strftime("%Y-%m-%d"),
    "total_attempts": total_attempts,
    "successful_attempts": successful_attempts,
    "unique_users": unique_users,
    "run_attempts": attempts_per_type.get("run", 0),
    "submit_attempts": attempts_per_type.get("submit", 0),
    "users_count": attempts_per_user_count
}

logging.info(f"Агрегированные данные: {daily_stats}")

try:
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file("credentials.json", scopes=scope)
    gc = gspread.authorize(creds)

    # Открываем таблицу и лист
    sheet = gc.open_by_url("https://docs.google.com/spreadsheets/d/1o9Iq4z47WwGdB1WnwogeE5IBuS2iorJPDbJCkEavg-A/edit?gid=0#gid=0").sheet1

    # Формируем строку
    sheet_row = [
        daily_stats["date"],
        daily_stats["total_attempts"],
        daily_stats["successful_attempts"],
        daily_stats["unique_users"],
        daily_stats["run_attempts"],
        daily_stats["submit_attempts"],
        daily_stats["users_count"]
    ]

    sheet.append_row(sheet_row)
    logging.info("Данные успешно загружены в Google Sheets")

except Exception as e:
    logging.error(f"Ошибка при работе с Google Sheets: {e}")
