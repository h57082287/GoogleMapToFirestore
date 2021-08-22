from typing import Tuple
from google.cloud.firestore_v1 import base_document
from google.cloud.firestore_v1.base_document import BaseDocumentReference
from firebase_admin import credentials
from firebase_admin import firestore
import firebase_admin

# 建立金鑰
cred = credentials.Certificate('serviceAccount.json')

# 建立憑證
firebase_admin.initialize_app(cred)

# 建立db的連線
db = firestore.client()

db.collection('資料庫','test','test').document('test').set({"path":db.collection('資料庫','地區','基隆').document('三姊妹')})

