import requests
import xml.etree.ElementTree as ET
import mysql.connector
from datetime import datetime
import os
from dotenv import load_dotenv

# Загружаем переменные из файла .env.dacha_info
dotenv_path = "/Users/dlm_air/Documents/GitHub/DLM_repository/invest_loaders/.env.dacha_info"  # Путь к файлу с переменными окружения
load_dotenv(dotenv_path=dotenv_path)  
# Читаем переменные
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

def check_existing_rates(rate_date, connection):
    """
    Проверка наличия записи с указанной датой в базе данных.
    """
    cursor = connection.cursor()

    # SQL-запрос для проверки существования записи
    sql = "SELECT COUNT(*) FROM currency.exchange_rates WHERE rate_date = %s"
    cursor.execute(sql, (rate_date,))
    result = cursor.fetchone()[0]  # Получаем количество записей с данной датой

    cursor.close()
    return result > 0  # Возвращает True, если запись существует



def get_exchange_rates(date):
    """
    Получение курсов валют с сайта ЦБ РФ.
    """
    url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={date.strftime('%d/%m/%Y')}"
    response = requests.get(url)
    response.raise_for_status()  # Проверяем, что запрос успешен
    return response.content

def parse_exchange_rates(xml_data):
    """
    Парсинг XML-данных с курсами валют.
    """
    tree = ET.ElementTree(ET.fromstring(xml_data))
    root = tree.getroot()
    rates = {'RUR': 1.0000}  # Базовая валюта - рубль
    for valute in root.findall('Valute'):
        char_code = valute.find('CharCode').text
        value = float(valute.find('Value').text.replace(',', '.'))
        nominal = int(valute.find('Nominal').text)
        rates[char_code] = value / nominal
    return rates

def insert_into_db(date, rates, connection):
    """
    Вставка курсов валют в базу данных.
    """
    cursor = connection.cursor()

    # Подготовка данных для вставки
    sql = """
    INSERT INTO currency.exchange_rates (rate_date, RUR, USD, GBP, EUR, KZT)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, (
        date,
        rates.get('RUR', None),
        rates.get('USD', None),
        rates.get('GBP', None),
        rates.get('EUR', None),
        rates.get('KZT', None)
    ))

    connection.commit()
    cursor.close()


def main():
    # Ввод даты (можно заменить на автоматическое получение сегодняшней даты)
    rate_date = datetime.now().date()  # Или: rate_date = datetime.strptime("2025-03-14", "%Y-%m-%d").date()

    # Открываем соединение с базой данных один раз
    connection = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

    try:
        # Проверяем наличие записи в базе данных
        if check_existing_rates(rate_date, connection):
            print(f"Запись с датой {rate_date} уже существует в базе данных. Завершаем выполнение.")
            return

        # Получаем курсы валют
        try:
            xml_data = get_exchange_rates(rate_date)
            rates = parse_exchange_rates(xml_data)
            print("Курсы валют успешно получены:", rates)
        except Exception as e:
            print("Ошибка при получении курсов валют:", e)
            return

        # Вставляем данные в базу
        try:
            insert_into_db(rate_date, rates, connection)
            print("Данные успешно внесены в базу.")
        except Exception as e:
            print("Ошибка при внесении данных в базу:", e)

    finally:
        # Закрываем соединение с базой данных
        connection.close()

if __name__ == "__main__":
    main()