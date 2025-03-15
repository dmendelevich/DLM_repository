import requests
import xml.etree.ElementTree as ET
import mysql.connector
from datetime import datetime
import time
import os
from dotenv import load_dotenv

dotenv_path = "/Users/dlm_air/Documents/GitHub/DLM_repository/invest_loaders/.env.dacha_info"  # Путь к файлу с переменными окружения
load_dotenv(dotenv_path=dotenv_path)

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

def get_empty_records(connection):
    """
    Получение записей с NULL для всех валют, кроме рубля.
    """
    cursor = connection.cursor(dictionary=True)
    sql = """
    SELECT rate_id, rate_date
    FROM currency.exchange_rates
    WHERE USD IS NULL AND GBP IS NULL AND EUR IS NULL AND KZT IS NULL
    """
    cursor.execute(sql)
    records = cursor.fetchall()
    cursor.close()
    return records

def get_exchange_rates(date):
    """
    Получение курсов валют с сайта ЦБ РФ.
    """
    url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={date.strftime('%d/%m/%Y')}"
    response = requests.get(url)
    response.raise_for_status()
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

def update_rates_in_db(rate_id, rates, connection):
    """
    Обновление курсов валют в записи таблицы.
    """
    cursor = connection.cursor()
    sql = """
    UPDATE currency.exchange_rates
    SET USD = %s, GBP = %s, EUR = %s, KZT = %s
    WHERE rate_id = %s
    """
    cursor.execute(sql, (
        rates.get('USD', None),
        rates.get('GBP', None),
        rates.get('EUR', None),
        rates.get('KZT', None),
        rate_id
    ))
    connection.commit()
    cursor.close()

def main():
    # Открываем соединение с базой данных
    connection = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

    try:
        # Получаем записи с пустыми курсами
        empty_records = get_empty_records(connection)
        if not empty_records:
            print("Нет записей с пустыми курсами валют.")
            return
        else:
            print(f"Найдено записей с пустыми курсами валют: {len(empty_records)}")


        results = []

        for record in empty_records:
            rate_id = record['rate_id']
            rate_date = record['rate_date']

            try:
                # Получаем курсы валют с сайта ЦБ РФ
                xml_data = get_exchange_rates(rate_date)
                rates = parse_exchange_rates(xml_data)
                #print(f"Курсы валют для даты {rate_date}: {rates}")

                # Обновляем запись в базе данных
                update_rates_in_db(rate_id, rates, connection)

                results.append(f"rate_id: {rate_id}, rate_date: {rate_date}, updated_rates: {rates}")
                time.sleep(1)  # Задержка, чтобы не нагружать сервер ЦБ РФ
                
            except Exception as e:
                print(f"Ошибка при обработке записи с rate_id {rate_id}: {e}")
                results.append(f"rate_id: {rate_id}, rate_date: {rate_date}, error: {str(e)}")

        # Записываем результаты в файл
        with open("empty_rates_results.txt", "w") as f:
            for line in results:
                f.write(line + "\n")

    finally:
        # Закрываем соединение с базой данных
        connection.close()

if __name__ == "__main__":
    main()