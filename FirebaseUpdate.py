# 引入套件
import csv
import time
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


# 讀取檔案
with open('output-基隆.csv') as f:
    rows = csv.reader(f)
    for row in rows:
        if len(row) != 0:
            print(row[1])
        

