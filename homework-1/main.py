"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import os
import psycopg2

# подключение к БД north
conn = psycopg2.connect(
    host='localhost',
    database='north',
    user='postgres',
    password='12345'
)

# пути к файлам с данными
path_file = os.path.abspath('../homework-1/north_data')
path_customers = os.path.join(path_file, 'customers_data.csv')
path_employees = os.path.join(path_file, 'employees_data.csv')
path_orders = os.path.join(path_file, 'orders_data.csv')

# добавляем данные в customers
try:
    with open(path_customers, mode='r', encoding='utf-8') as f:
        file_reader = csv.DictReader(f, delimiter=',')
        for fr in file_reader:
            with conn:
                with conn.cursor() as cur:
                    cur.execute('INSERT INTO customers VALUES (%s, %s, %s)',
                                (fr['customer_id'], fr['company_name'], fr['contact_name']))

finally:
    conn.close()

# добавляем данные в employees
try:
    with open(path_employees, mode='r', encoding='utf-8') as f:
        file_reader = csv.DictReader(f, delimiter=',')
        for fr in file_reader:
            with conn:
                with conn.cursor() as cur:
                    cur.execute('INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)',
                                (fr['employee_id'], fr['first_name'], fr['last_name'], fr['title'], fr['birth_date'], fr['notes']))

finally:
    conn.close()

# добавляем данные в orders
try:
    with open(path_orders, mode='r', encoding='utf-8') as f:
        file_reader = csv.DictReader(f, delimiter=',')
        for fr in file_reader:
            with conn:
                with conn.cursor() as cur:
                    cur.execute('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)',
                                (fr['order_id'], fr['customer_id'], fr['employee_id'], fr['order_date'], fr['ship_city']))

finally:
    conn.close()