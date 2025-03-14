import os
from dotenv import load_dotenv

# Загружаем переменные из файла .env.dacha_info
dotenv_path = "/Users/dlm_air/Documents/GitHub/DLM_repository/invest_loaders/.env.dacha_info"  # Путь к файлу с переменными окружения
load_dotenv(dotenv_path=dotenv_path)  # Загружаем переменные из указанного файла


# Читаем переменные
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Выводим значения для проверки (только для тестирования, не делай так в реальном проекте!)
print(f"Host: {DB_HOST}, User: {DB_USER}, Password: {DB_PASSWORD}, Database: {DB_NAME}")

# Пример подключения к MySQL
import mysql.connector
connection = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)
print("Подключение к базе данных успешно установлено!")