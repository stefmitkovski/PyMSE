from flask import Flask, jsonify, request
from pymongo import MongoClient, errors
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import  sys, os, re
from bson.json_util import dumps
from datetime import datetime, timedelta
from dotenv import load_dotenv
from async_tasks import processing_reports_async, downloading_reports_async
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

load_dotenv()

# GLOBAL VARIBLES
MONGODB_HOST = os.getenv("HOST")
MONGODB_PORT = int(os.getenv("PORT"))
DB_NAME = os.getenv("DB")
REPORTS_DIRECTORY = os.getenv("REPORTS_DIRECTORY")
STARTING_DATE = int(os.getenv("STARTING_DATE"))
NUM_THREADS = int(os.getenv("NUM_THREADS"))

# INITIALIZATION
mongo_client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = mongo_client[DB_NAME]

# Zemanje na najnovi informacii na momentalno trguvani akcii
@app.route('/api/reports/latest')
def latest():
    reports = db['reports']
    current_date = datetime.now()
    last_date = datetime(STARTING_DATE, 1, 1)
    while current_date >= last_date:
        print(f"Poceten datum: {current_date}, posleden datum: {last_date}")
        current_date_formated = current_date.replace(hour=0, minute=0, second=0, microsecond=0)

        exists = list(reports.find({'date': current_date_formated},{'_id':0}))
        
        if len(exists) > 0:
            return jsonify(dumps(exists))
        current_date -= timedelta(days=1)
            
    return jsonify({"ok":True}),200


# API endpoint za vrakanje na informaciija za odredena firma
@app.route('/api/reports/search', methods=['POST'])
def search_report():
    fromDate = ''
    toDate = ''
    symbol = ''
    if 'symbol' in request.json:
        symbol = request.json['symbol']

    if 'from' in request.json:
        fromDate = request.json['from']
    else:
        fromDate = datetime(STARTING_DATE, 1, 1).strftime('%Y-%m-%d')
        
    if 'to' in request.json:
        toDate = request.json['to']
    else:
        toDate = datetime.now().strftime("%Y-%m-%d")
    
    reports = db['reports']
    if symbol == '':
        results = reports.find({
            "date": {
                "$gte": datetime.strptime(fromDate, "%Y-%m-%d"),
                "$lte": datetime.strptime(toDate, "%Y-%m-%d")
                }
            })
    else:
        results = reports.find({
            "symbol": symbol,
            "date": {
                "$gte": datetime.strptime(fromDate, "%Y-%m-%d"),
                "$lte": datetime.strptime(toDate, "%Y-%m-%d")
                }
            })
        
    response_data = []

    for document in results:
        del document["_id"]
        response_data.append(document)

    return jsonify(response_data), 200

# API za procesiranje na izestaite
@app.route('/api/reports/process', methods=['POST'])
def processing_reports():
    print('Zapocnuvanje na prerabotkata na izevstaite')
    try:
        list_of_reports = list_reports(fromRequest=False)
        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            executor.map(processing_reports_async,list_of_reports)
        
        return jsonify({"ok":True}),200
    except:
        print("Greska pri prevzemanje na izvestaite")
        return jsonify({"error":True}),200

            
    
# Simnigi site izvestai sto falat
@app.route('/api/reports/download', methods=['POST'])
def download():
    current_reports = list_reports(fromRequest=False)    
    start_date = datetime(STARTING_DATE, 1, 1)
    current_date = datetime.now()
    date_arr = []
    current = start_date
    while current <= current_date:
        date = current.strftime("%Y-%m").split("-")
        date_arr.append(date)
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(downloading_reports_async, date_arr,[current_reports] * len(date_arr))

    return jsonify("ok")


# Listanje na site izestai koi go ima na prilog serverot
@app.route('/api/reports/lists')
def list_reports(fromRequest = True):
    if not os.path.exists(REPORTS_DIRECTORY):
        os.makedirs(REPORTS_DIRECTORY)
        if fromRequest:
            return jsonify({"reports":None}),200
        else:
            return None
         
    files = [
                re.sub(r'\.[^.]+$', '', f)  # Trgni sve posle prvata potcka
                for f in os.listdir(REPORTS_DIRECTORY)
                if os.path.isfile(os.path.join(REPORTS_DIRECTORY, f)) 
            ]
    if fromRequest:
        return jsonify({"reports":files}),200
    else:
        return files

# Vrati lita na firmi koi se pojavuvaat vo izvestaite
# Za testiranje samo
# @app.route('/api/list_reports')
# def func_list_reports():
#     file_path = 'reports/'
    
#     list_of_reports = list_reports(fromRequest=False)
#     list_companies = []
    
#     for report in list_of_reports:
#         temp_path = file_path + report + ".xls"

#         try:
#             report_content = pd.read_excel(temp_path)
#         except:
#             print(f"Nemoze da se pristapi izvestajot {report}")
#             continue

#         for _, row in report_content.iterrows():

#             if pd.isna(row.iloc[1]):
#                 continue

#             company_name = str(row.iloc[0]).strip()
#             if company_name not in list_companies:
#                 list_companies.append(company_name)
                
#     return jsonify(list_companies), 200
    
if __name__ == '__main__':
    
    # Kreiranje na key-value databaza vo mongodb
    try:
        df = pd.read_csv("companies.csv")
        key_value_data = [
            {"key": row[1].strip().lower(),
             "value": row[0]} for row in df.values
        ]

        collection = db['companies']

        inserted_count = 0
        for record in key_value_data:
            # Proveri dali postoi veke takov kluc
            existing_doc = collection.find_one({"key": record["key"]})
            # Ako ne postoi ne go vnesuvaj vo baza
            if not existing_doc:
                collection.insert_one(record)
                inserted_count += 1
        if inserted_count > 0:
            print("Inserted {inserted_count} new companies")
    except FileNotFoundError:
        print("Error: 'companies.csv' file not found.")
        sys.exit(1)

    except errors.ServerSelectionTimeoutError:
        print("Error: Unable to connect to MongoDB. Please check if the server is running.")
        sys.exit(1)

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)
        
    app.run(host='0.0.0.0',port=5000,debug=True)