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

    dj.connect(scheme='sqlite', database="data_juggler_test.sqlite3")
    dj.connect(scheme='sqlite', database="chinook.db")
    # dj.db = cnxn

    dj.open(query="select * from albums", query_name="reports")
    print(dj.data)
    # dj.join("data")
    # print(dj.to_json("data"))
