# 引入套件
import csv
import time
import firebase_admin
from google.cloud import firestore
from firebase_admin import credentials
from firebase_admin import firestore
import random
import string

# 輸入地區
localcation = input('請輸入地區:')

# 建立金鑰
cred = credentials.Certificate('serviceAccount.json')

# 建立憑證
firebase_admin.initialize_app(cred)

# 建立db的連線
db = firestore.client()

# 建立全域變數
## 電話判別
Tel = ['02','03','04','05','06','07','08','09']
## 餐廳分類歸納
newClasses = {
    "小吃快餐":["熟食店","中式麵食店","麵店","餃子店","中式包點店,","冷麵店","小吃店","粥餐廳","快餐店","烤雞速食店","豆腐餐廳"],
    "早午餐":["早午餐餐廳","三文治店","早餐餐廳"],
    "中式料理":["中餐館","台灣餐廳","海鮮餐廳","中菜館","鐵板燒餐廳","雞肉餐廳","亞洲菜餐廳","家常菜餐廳","粵菜館","客家菜館","農家菜館","亞洲","Fusion 菜餐廳","京菜/北京菜館","閩菜/福建菜館","滬菜館","鲁菜/山東菜館"],
    "下午茶":["咖啡廳","咖啡店","麵包西餅店","冰品飲料店","甜品店","甜品餐廳","中式糕餅店","珍珠奶茶店","果汁店","雪糕店"],
    "火鍋":["火鍋餐廳","涮涮鍋餐廳"],
    "日本料理":["日本餐廳","拉麵店","壽司店","日式燒肉餐廳","日式咖哩餐廳","日式烤雞串餐廳","迴轉壽司餐廳","壽喜燒和日式火鍋餐廳","牛丼餐廳","日本地方料理餐廳","正宗日式料理餐廳","日式炸豬扒餐廳","關東煮餐廳","日式牛扒餐廳","日式串燒餐廳","壽喜燒餐廳","天婦羅餐廳","日本熟食店","海鮮丼餐廳","章魚燒餐廳"],
    "素食":["素食餐廳","純素餐廳"],
    "義式料理":["意大利餐廳","意大利麵食店","薄餅餐廳"],
    "美式西餐":["美式牛扒屋","美式扒房","美式餐廳","漢堡餐廳","現代美式餐廳","傳統美式餐廳","西餐廳","中美洲菜餐廳","巴西餐廳","墨西哥餐廳","牛排"],
    "泰式料理":["泰國餐廳"],
    "燒烤":["炸物串與串炸餐廳","串燒烤肉店","燒烤餐廳","酒吧扒房","燒烤","燒烤場"],
    "韓式料理":["韓國餐廳","韓式燒烤餐廳"],
    "越南料理":["越南餐廳","越式河粉餐廳"],
    "自助餐":["自助餐廳","自助餐餐廳"],
    "港式料理":["港式快餐店"],
    "西班牙料理":["西班牙小食餐廳"],
    "海鮮":["海鮮餐廳"],
    "健康輕食料理":["健康食品餐廳","輕食餐廳"],
    "印度料理":["印度餐廳","印度香飯餐廳"],
    "法式料理":["法式餐廳"],
    "酒吧":["酒吧"],
}

# 亂數編號生成器
def RandomID(Len = 50):
    random_str = ''.join(random.sample(string.ascii_letters + string.digits, Len))
    return random_str

def UpdateDatabase(mod,table,sn,data):
    if mod == '1':
        db.collection('資料庫',table,localcation).document(sn).set(data)
    elif mod == '2':
        db.collection('資料庫',table,'詳細資料').document(sn).set(data)

# 讀取檔案並傳送至firebase
with open('output-'+ localcation +'.csv') as f:
    rows = csv.reader(f)
    for row in rows:
        #print(row)
        # 部分區域變數宣告區
        data = {"meum":[],"image":[],"discTopic":[]}
        disc = {}
        buffer_Map = {}
        idNum = []
        ElementNum = 0
        imageNum = 0
        latitude = 0.00
        longitude = 0.00
        
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
                        latitude = float(element)
                    elif (int(element.split('.')[0]) >= 22):
                        longitude = float(element)
                        location=firestore.GeoPoint(longitude, latitude)
                        data['latlnt'] = location
                    ElementNum += 1
                # 檢測總評論
                elif element.find('則評論') != -1:
                    data['totalDisc'] = element.split('則')[0]
                    ElementNum += 1
                # 檢測評論(特殊說明: 因為每則評論皆獨立欄位，因此加入傳送的功能放到下一個檢測項目的開始)
                elif (element.find('\n') != -1) and (element.find('https://') == -1) and (ElementNum < len(row)-4):
                    id  = RandomID()
                    buffer_Map['Name'] = element.split('\n')[0]
                    buffer_Map['Star'] = element.split('\n')[1]
                    try:
                        buffer_Map['disc'] = element.split('\n')[2]
                    except:
                        pass
                    #disc[id] = buffer_Map
                    UpdateDatabase('2','評論',id,buffer_Map)
                    idNum.append(id)
                    ElementNum += 1
                # 檢測圖片
                elif (element.find('\n') != -1) and (element.find('https://') != -1) and (imageNum < 1):
                    # 執行評論的id上傳
                    data['disc'] = idNum
                    # 圖片處理
                    meumImage = element.split('\n')
                    data['meum'] = meumImage 
                    imageNum += 1
                    ElementNum += 1
                # 檢測菜單
                elif (element.find('\n') != -1) and (element.find('https://') != -1) and (imageNum >= 1):
                    image = element.split('\n')
                    data['image'] = image
                    ElementNum += 1
                # 檢測評論主題
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
            # 上傳資料
            """
            Update_Data = {}
            Update_Data[data['StoreName']] = data
            print(Update_Data)
            """
            print(data)
            # 嘗試上傳
            try:
                UpdateDatabase('1','地區',data['StoreName'],data)
            except:
                print("發生錯誤:" + data['StoreName'])
                with open('log.txt','a+') as f:
                    f.write('發生上傳錯誤('+ data['StoreName']+')\n')
        #break

            



