# 引入套件
import csv
import time
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


# 建立金鑰
cred = credentials.Certificate('serviceAccount.json')

# 建立憑證
firebase_admin.initialize_app(cred)

# 建立db的連線
db = firestore.client()


# 讀取檔案並傳送至firebase
with open('output-基隆.csv') as f:
    rows = csv.reader(f)
    for row in rows:
        #print(row)
        data = {}
        if len(row) != 0:
            # 固定元素區
            data['StoreName'] = row[0]
            data['Star'] = row[1]

            # 非固定元素區
            for element in row:
                print(element)
                # 檢測是否為餐廳主題
                if ((element.find('店')) != -1 or (element.find('餐廳') != -1) or (element.find('館') != -1) or (element.find('燒烤') != -1)) and len(element) < 4:
                    data['StoreClasses'] = element
                # 檢測是否為營業時間
                elif (element.find('星期') != -1) or (element.find('尚未提供時間')!= -1):
                    buffer_Time = []
                    if element.find('尚未提供時間') != -1 :
                       data['StoreTime'] = {'Business' : False,'OpenTime':'尚未提供'}
                    else:
                        DT = element.split('\n')
                        #print(DT)
                        for d in DT:
                            if d.find('休息') == -1:
                                 buffer_Time.append({'Business' : True,'OpenTime':d[3:]})
                            else:
                                buffer_Time.append({'Business' : False,'OpenTime':'休息'})
                        data['StoreTime'] = buffer_Time
                # 檢測運送方式
                elif (element.find('外送') != -1) or (element.find('外帶') != -1) or (element.find('內用') != -1) or ((element.find('尚未提供') != -1)) :
                    if element.find('尚未提供') != -1 :
                        data['method'] = '尚未提供'
                    else:
                        m = element.split('\n')
                        data['method'] = m
                # 檢測地址
                elif ((element.find('市') != -1) or (element.find('縣') != -1)):
                    data['address'] = element
                    if element.find('鄉') != -1:
                        data['local'] = element[(element.find('鄉')-2):element.find('鄉') + 1 ]
                    elif element.find('區') != -1:
                        data['local'] = element[(element.find('區')-2):element.find('區') + 1 ]
                    elif element.find('鎮') != -1:
                        data['local'] = element[(element.find('鎮')-2):element.find('鎮') + 1 ]
                    elif element.find('市') != -1:
                        data['local'] = element[(element.find('市')-2):element.find('市') + 1 ]
                # 檢測電話
        break
    print(data)

            



