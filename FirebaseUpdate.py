# 引入套件
import csv
import time
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import random
import string


# 建立金鑰
cred = credentials.Certificate('serviceAccount.json')

# 建立憑證
firebase_admin.initialize_app(cred)

# 建立db的連線
db = firestore.client()

# 建立全域變數
Tel = ['02','03','04','05','06','07','08','09']

# 亂數編號生成器
def RandomID(Len = 50):
    random_str = ''.join(random.sample(string.ascii_letters + string.digits, Len))
    return random_str

# 讀取檔案並傳送至firebase
with open('output-基隆.csv') as f:
    rows = csv.reader(f)
    for row in rows:
        #print(row)
        # 部分區域變數宣告區
        data = {}
        disc = {}
        idNum = []
        ElementNum = 0
        imageNum = 0

        if len(row) != 0:
            # 固定元素區
            data['StoreName'] = row[0]
            data['Star'] = row[1]
            ElementNum += 2
            # 非固定元素區
            for element in row:
                #print(element)
                # 檢測是否為餐廳主題
                if ((element.find('店')) != -1 or (element.find('餐廳') != -1) or (element.find('館') != -1) or (element.find('燒烤') != -1)) and len(element) < 4:
                    data['StoreClasses'] = element
                    ElementNum += 1
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
                    ElementNum += 1
                # 檢測運送方式
                elif (element.find('外送') != -1) or (element.find('外帶') != -1) or (element.find('內用') != -1) or ((element.find('尚未提供') != -1)) :
                    if element.find('尚未提供') != -1 :
                        data['method'] = '尚未提供'
                    else:
                        m = element.split('\n')
                        data['method'] = m
                    ElementNum += 1
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
                    ElementNum += 1
                # 檢測電話
                elif (element[:2] in Tel) and (len(element.replace(' ','')) <= 10) :
                    data['Tel'] = element.replace(' ','')
                    ElementNum += 1
                # 檢測經緯度
                elif (element.split('.')[0].isdigit()) and (element.find('.') != -1) and (len(element.split('.')) <= 2) and (int(element.split('.')[0]) >= 22):
                    if (int(element.split('.')[0]) >= 119):
                        data['lat'] = element
                    elif (int(element.split('.')[0]) >= 22):
                        data['lng'] = element
                    ElementNum += 1
                # 檢測總評論
                elif element.find('則評論') != -1:
                    data['totalDisc'] = element.split('則')[0]
                    ElementNum += 1
                # 檢測評論(特殊說明: 因為每則評論皆獨立欄位，因此加入傳送的功能放到下一個檢測項目的開始)
                elif (element.find('\n') != -1) and (element.find('https://') == -1) and (ElementNum < len(row)-4):
                    id  = RandomID()
                    buffer_Map = {}
                    buffer_Map['Name'] = element.split('\n')[0]
                    buffer_Map['Star'] = element.split('\n')[1]
                    buffer_Map['disc'] = element.split('\n')[2]
                    disc[id] = buffer_Map
                    idNum.append(id)
                    ElementNum += 1
                # 檢測菜單
                elif (element.find('\n') != -1) and (element.find('https://') != -1) and (imageNum < 1):
                    # 執行評論的id上傳
                    data['disc'] = idNum
                    # 菜單圖片處理
                    meumImage = element.split('\n')
                    data['meum'] = meumImage 
                    imageNum += 1
                    ElementNum += 1
                # 檢測圖片
                elif (element.find('\n') != -1) and (element.find('https://') != -1) and (imageNum >= 1):
                    image = element.split('\n')
                    data['image'] = image
                    ElementNum += 1
                # 檢測評論
                elif (ElementNum == len(row)-2) and (element.find('https://') == -1 ) and (element.find('尚未提供照片') == -1):
                    buffer_discTopic = []
                    discTopic = element.split('\n')
                    for d in discTopic:
                        dd = {}
                        try:
                            topicName = d.split(' ')[0]
                            num = d.split(' ')[1]
                        except:
                            continue
                        dd['topicName'] = topicName
                        dd['num'] = num
                        buffer_discTopic.append(dd)
                    data['discTopic'] = buffer_discTopic
        #print(len(row))
        #print(ElementNum)
        print(data)
        break

            



