# Загрузка данных о сделках в MySQL

import os
from dotenv import load_dotenv
import pandas as pd
import mysql.connector
import sys
from datetime import datetime


# 1. Подключение к MySQL
# Загружаем переменные из файла .env.dacha_info
dotenv_path = "/Users/dlm_air/Documents/GitHub/DLM_repository/invest_loaders/.env.dacha_info"  # Путь к файлу с переменными окружения
load_dotenv(dotenv_path=dotenv_path)  # Загружаем переменные из указанного файла
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
    # Удаляем лишние пробелы в заголовках
    data_frame.columns = data_frame.columns.str.strip()

    cursor = connection.cursor()

    # Счётчики
    total_rows = len(data_frame)  # Общее количество строк в DataFrame
    inserted_rows = 0  # Счётчик успешно добавленных строк

    for index, row in data_frame.iterrows():
        try:
            # Преобразование данных
            status = row['Статус'].strip() if pd.notnull(row['Статус']) else None
            operation = row['Операция'].strip() if pd.notnull(row['Операция']) else None
            ticker = row['Тикер'].strip() if pd.notnull(row['Тикер']) else None

            price = float(row['Цена']) if isinstance(row['Цена'], (int, float)) else float(row['Цена'].replace(' ', '').replace(',', '.')) if pd.notnull(row['Цена']) else None
            qty = int(row['Количество']) if pd.notnull(row['Количество']) else None

            amount = None
            if pd.notnull(row['Сумма']):
                amount = (
                    float(row['Сумма']) if isinstance(row['Сумма'], (int, float))
                    else float(row['Сумма'].replace(' ', '').replace('$', '').replace('~', '').replace(',', '.').strip())
                    if 'данных' not in row['Сумма'].lower()
                    else None
                )

            qty_remaining = int(row['Остаток']) if pd.notnull(row['Остаток']) else None
            order_type = row['Тип приказа'].strip() if pd.notnull(row['Тип приказа']) else None

            order_condition = None
            if pd.notnull(row['Условие']):
                order_condition = (
                    float(row['Условие']) if isinstance(row['Условие'], (int, float))
                    else float(row['Условие'].replace(' ', '').replace(',', '.'))
                    if row['Условие'] != '-'
                    else None
                )

            expiry = row['Срок'].strip() if pd.notnull(row['Срок']) else None

            order_date = datetime.strptime(row['Время'], '%Y-%m-%d %H:%M:%S') if pd.notnull(row['Время']) else None
            order_number = (
                int(row['№  приказа'])
                if isinstance(row['№  приказа'], (int, float))
                else int(row['№  приказа'].replace(' ', ''))
                if pd.notnull(row['№  приказа'])
                else None
            )

            # SQL для вставки данных
            sql = f"""
                INSERT IGNORE INTO {table_name} (
                    status,
                    operation,
                    ticker,
                    price,
                    qty,
                    amount,
                    qty_remaining,
                    order_type,
                    order_condition,
                    expiry,
                    order_date,
                    order_number
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            values = (
                status,
                operation,
                ticker,
                price,
                qty,
                amount,
                qty_remaining,
                order_type,
                order_condition,
                expiry,
                order_date,
                order_number
            )

            # Выполнение запроса
            cursor.execute(sql, values)

            # Увеличиваем счётчик, если строка была добавлена
            if cursor.rowcount > 0:
                inserted_rows += 1

        except Exception as e:
            print(f"Ошибка при обработке строки {index + 1}: {e}")

    # Фиксируем изменения
    connection.commit()
    cursor.close()

    # Итоговый вывод
    print(f"Общее количество строк в DataFrame: {total_rows}")
    print(f"Успешно добавлено строк в базу данных: {inserted_rows}")
    print(f"Проигнорировано строк (возможно, дубликаты): {total_rows - inserted_rows}")

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
    table_name = "invest.orders"

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

    finally:
        # Закрываем соединение с MySQL
        connection.close()


if __name__ == "__main__":
    main()