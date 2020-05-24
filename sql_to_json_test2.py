import sqlite3
import pyodbc
from data_juggler import data_juggler


if __name__ == '__main__':
    dj = data_juggler()
    # dj.connect()
    # conn = sqlite3.connect("data_juggler_test.sqlite3")
    # cursor = conn.cursor()
    #
    # cursor.execute("""CREATE TABLE albums
    #                   (title TEXT, artist TEXT, release_date TEXT,
    #                    publisher TEXT, media_type TEXT)
    #                """)
    #
    # cursor.execute("""INSERT INTO albums
    #                   VALUES ('Glow', 'Andy Hunter', '7/24/2012',
    #                   'Xplore Records', 'MP3')"""
    #                )
    # conn.commit()
    # cnxn = pyodbc.connect("Driver=SQLite3 ODBC Driver;Database=data_juggler_test.sqlite3")
    # dj.db = cnxn

    # dj.data[Name of Sets][Number set][Number row][Name cell]
    # dj.connect(scheme='sqlite', database="data_juggler_test.sqlite3")
    dj.connect(scheme='sqlite', database="chinook.db")

    dj.data['invoices']=[[{"sheet1:": 1, "sheet2:": 2}]]

    dj.open(query="select  *, InvoiceId as [items:], 1 as [:sheet1]  from invoices limit 2", query_name="invoices")
    dj.open(query="select *, InvoiceId as [:items]  from invoice_items", query_name="invoices")

    # print(dj.data['invoices'][0])
    # print(dj.data['invoices'][1])

    dj.join('invoices')
    json = dj.to_json("invoices")
    # print(dj.data['invoices'][0])
    print(json)

    # print(dj.to_json("data"))
