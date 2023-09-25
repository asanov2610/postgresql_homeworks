"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2


import csv

conn = psycopg2.connect(
    host="localhost",
    database="north",
    user="postgres",
    password="Manchester2610@"
)

try:
    with conn:
        with conn.cursor() as cur:
            with open('north_data\customers_data.csv', 'r', encoding='UTF-8') as file_csv:
                next(file_csv)
                for line in file_csv:
                    new_line = line.replace('"', '')
                    cur.executemany('INSERT INTO customers VALUES (%s, %s, %s)', [new_line.split(',')])
            file_csv.close()

            with open('north_data\employees_data.csv', 'r', encoding='UTF-8') as file_csv:
                next(file_csv)
                for line in file_csv:
                    new_line = line.replace('"', '')
                    cur.executemany('INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)', [new_line.split(',', 5)])
            file_csv.close()

            with open('north_data\orders_data.csv', 'r', encoding='UTF-8') as file_csv:
                next(file_csv)
                for line in file_csv:
                    new_line = line.replace('"', '')
                    cur.executemany('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)',[new_line.split(',')])
            file_csv.close()

finally:
    conn.close()



