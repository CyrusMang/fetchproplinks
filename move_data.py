# import csv
import os
import mysql.connector
from pymongo import MongoClient
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from models.prop import Prop
from models.prop_meriadb import PropMariaDB
# from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

MARIADB_HOST = os.getenv("MARIADB_HOST")
MARIADB_PORT = os.getenv("MARIADB_PORT", "3306")
MARIADB_USER = os.getenv("MARIADB_USER")
MARIADB_PASSWORD = os.getenv("MARIADB_PASSWORD")
MARIADB_DATABASE = os.getenv("MARIADB_DATABASE")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")

mariadb = mysql.connector.connect(
    host=MARIADB_HOST,
    port=int(MARIADB_PORT),
    user=MARIADB_USER,
    password=MARIADB_PASSWORD,
    database=MARIADB_DATABASE
)

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
client = MongoClient(MONGODB_CONNECTION_STRING)
mongodb = client['prop_main']

last = 0
batch_size = 100

def move_data():
    global last
    while True:
        props = Prop.batch(mongodb, last, batch_size)
        if not props:
            print("No more props to move.")
            break
        for prop in props:
            data = prop.data
            source_id = data['source_id']
            try:
                PropMariaDB.create_or_update(mariadb, source_id, data)
            except Exception as e:
                print(f"Error processing prop {source_id}: {e}")
        last += len(props)

if __name__ == '__main__':
    move_data()