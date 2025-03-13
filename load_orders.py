#!/usr/local/bin/python3

import pandas as pd
import mysql.connector
import sys
from datetime import datetime


# 1. Подключение к MySQL
def connect_to_mysql():
    connection = mysql.connector.connect(
        host="89.104.117.98",
        user="root_user",
        password="#DdLlMm24680",
        database="dacha_info"
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
    # print(data_frame.columns)

    cursor = connection.cursor()

    for index, row in data_frame.iterrows():
        try:
            # Преобразование данных
            status = row['Статус'].strip() if pd.notnull(row['Статус']) else None
            operation = row['Операция'].strip() if pd.notnull(row['Операция']) else None
            ticker = row['Тикер'].strip() if pd.notnull(row['Тикер']) else None

            # Обработка числовых полей с проверкой на тип
            price = float(row['Цена']) if isinstance(row['Цена'], (int, float)) else float(row['Цена'].replace(' ', '').replace(',', '.')) if pd.notnull(row['Цена']) else None
            qty = int(row['Количество']) if pd.notnull(row['Количество']) else None

            # Обработка "Сумма"
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
        print("Данные успешно добавлены в MySQL!")
    finally:
        # Закрываем соединение с MySQL
        connection.close()


if __name__ == "__main__":
    main()