# Загрузка данных о сделках в MySQL

import os
from dotenv import load_dotenv
import pandas as pd
import mysql.connector
import sys
import re
from datetime import datetime

currency_symbols = {
    '$': 'USD',
    '₸': 'KZT',
    '£': 'GBP',
    '€': 'EUR',
    '₽': 'RUR'
}

# 1. Подключение к MySQL

# Загружаем переменные из файла .env
load_dotenv()
# Читаем переменные
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

def connect_to_mysql():
    connection = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    return connection

# 2. Чтение данных из Excel
def read_excel(file_path):
    # Читаем Excel-файл в DataFrame
    df = pd.read_excel(file_path)
    return df

# 3. Добавление данных в MySQL с преобразованием
def insert_into_mysql(connection, table_name, data_frame):
    cursor = connection.cursor()

    for index, row in data_frame.iterrows():
        try:
            # Преобразование данных
            deal_number = int(row['№ сделки']) if pd.notnull(row['№ сделки']) else None
            order_number = int(row['№ приказа']) if pd.notnull(row['№ приказа']) else None
            datetime_value = datetime.strptime(row['Время'], '%d.%m.%Y %H:%M:%S') if pd.notnull(row['Время']) else None
            ticker = row['Тикер'] if pd.notnull(row['Тикер']) else None
            deal_type = 'buy' if row['Операция'].strip() == 'покупка' else 'sell' if row['Операция'].strip() == 'продажа' else None
            price = float(row['Цена']) if isinstance(row['Цена'], (int, float)) else float(row['Цена'].replace(',', '.')) if pd.notnull(row['Цена']) else None
            qty = int(row['Количество']) if pd.notnull(row['Количество']) else None
            amount = float(row['Сумма']) if isinstance(row['Сумма'], (int, float)) else float(row['Сумма'].replace(',', '.')) if pd.notnull(row['Сумма']) else None
            comission = (float(row['Комиссия']) if isinstance(row['Комиссия'], (int, float)) else float(re.sub(r'[^\d.,]', '', row['Комиссия']).replace(',', '.')) if pd.notnull(row['Комиссия']) else None)
            if pd.notnull(row['Комиссия']):
                comission_currency = next((currency_symbols[symbol] for symbol in currency_symbols if symbol in str(row['Комиссия'])), '???')
            profit = (float(row['Прибыль']) if isinstance(row['Прибыль'], (int, float)) else float(re.sub(r'[^\d.,]', '', row['Прибыль']).replace(',', '.')) if pd.notnull(row['Прибыль']) else None)
            if pd.notnull(row['Прибыль']):
                profit_currency = next((currency_symbols[symbol] for symbol in currency_symbols if symbol in str(row['Прибыль'])), '?') 
            
            # SQL для вставки данных
            sql = f"""
                INSERT IGNORE INTO {table_name} (
                    deal_number,
                    order_number,
                    datetime,
                    ticker,
                    deal_type,
                    price,
                    qty,
                    amount,
                    comission,
                    comission_currency,
                    profit,
                    profit_currency
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                deal_number,
                order_number,
                datetime_value,
                ticker,
                deal_type,
                price,
                qty,
                amount,
                comission,
                comission_currency,
                profit,
                profit_currency
            )

            # Отладочный вывод
            # print(f"SQL-запрос: {sql}")
            # print(f"Значения: {values}")

            cursor.execute(sql, values)
        
        except Exception as e:
            print(f"Ошибка при обработке строки {index + 1}: {e}")

    # Фиксируем изменения
    connection.commit()
    cursor.close()

# 4. Основная программа
def main():
    # Проверяем, передан ли путь к файлу в аргументах
    if len(sys.argv) < 2:
        print("Ошибка: путь к Excel-файлу не передан.")
        print("Использование: python3 load_deals.py <путь_к_файлу>")
        sys.exit(1)
    
    # Получаем путь к файлу из аргументов
    file_path = sys.argv[1]
    print(f"Обработан файл: {file_path}")

    # Имя таблицы в MySQL
    table_name = "invest.deals"

    # Подключаемся к MySQL
    connection = connect_to_mysql()

    try:
        # Читаем данные из Excel
        df = read_excel(file_path)
        print("Данные из Excel успешно загружены!")

        # Проверка на пустой DataFrame
        if df.empty:
            print("DataFrame пуст. Возможно, файл Excel некорректен.")
            sys.exit(1)

        # Добавляем данные в MySQL
        insert_into_mysql(connection, table_name, df)
        print("Данные успешно добавлены в MySQL!")
    finally:
        # Закрываем соединение с MySQL
        connection.close()


if __name__ == "__main__":
    main()