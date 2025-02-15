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
MONGODB_HOST = os.getenv("MONGODB_HOST")
MONGODB_PORT = int(os.getenv("MONGODB_PORT"))
HOST = os.getenv("HOST")
PORT = int(os.getenv("PORT"))
DB_NAME = os.getenv("DB")
REPORTS_DIRECTORY = os.getenv("REPORTS_DIRECTORY")
STARTING_DATE = int(os.getenv("STARTING_DATE"))
NUM_THREADS = int(os.getenv("NUM_THREADS"))

# INITIALIZATION
mongo_client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = mongo_client[DB_NAME]

@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, PUT, POST"
    response.headers["Access-Control-Allow-Headers"] = "Origin, X-Requested-With, Content-Type, Accept"
    return response

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

@app.route('/api/reports/week_report')
def week_report():
    reports = db['reports']
    current_date = datetime.now()
    last_date = datetime(STARTING_DATE, 1, 1)

    combined_data = {}
    days_collected = 0

    while current_date >= last_date and days_collected < 7:
        print(f"Start date: {current_date}, End date: {last_date}")
        current_date_formated = current_date.replace(hour=0, minute=0, second=0, microsecond=0)

        daily_records = list(reports.find({'date': current_date_formated}, {'_id': 0}))
        
        if daily_records:
            days_collected += 1
            for record in daily_records:
                symbol = record['symbol']
                if symbol not in combined_data:
                    combined_data[symbol] = record
                    combined_data[symbol]['appeard'] = 1
                else:
                    new_average_price = record.get('average_price', 0)
                    old_average_price = combined_data[symbol]['average_price']
                    appeard = combined_data[symbol]['appeard']
                    if new_average_price != 0:
                        combined_data[symbol]['average_price'] = ((appeard *old_average_price)+new_average_price)/(appeard+1)
                    combined_data[symbol]['change'] += record.get('change', 0.0)
                    combined_data[symbol]['appeard'] = appeard +1

                #     combined_data[symbol]['average_price'] += record.get('average_price', 0)
                #     combined_data[symbol]['change'] += record.get('change', 0.0)
                #     combined_data[symbol]['purchase_price'] += record.get('purchase_price', 0)
                #     combined_data[symbol]['sale_price'] += record.get('sale_price', 0)
                #     combined_data[symbol]['max'] += record.get('max', 0)
                #     combined_data[symbol]['min'] += record.get('min', 0)
                #     combined_data[symbol]['last_price'] = record.get('last_price', combined_data[symbol]['last_price'])
                #     combined_data[symbol]['quantity'] += record.get('quantity', 0)
                #     combined_data[symbol]['turnover_in_1000_den'] += record.get('turnover_in_1000_den', 0)
        
        current_date -= timedelta(days=1)

    return jsonify(dumps(list(combined_data.values())))

# API endpoint za vrakanje na informaciija za odredena firma
@app.route('/api/reports/search', methods=['POST'])
def search_report():
    fromDate = request.json.get("from", datetime(STARTING_DATE, 1, 1).strftime("%Y/%m/%d"))
    toDate = request.json.get("to", datetime.now().strftime("%Y/%m/%d"))
    symbols = request.json.get("symbol", [])
    reports = db["reports"]
    results = []
    if symbols:
        for symbol in symbols:
            cursor = reports.find(
                {
                    "symbol": symbol,
                    "date": {
                        "$gte": datetime.strptime(fromDate, "%Y/%m/%d"),
                        "$lte": datetime.strptime(toDate, "%Y/%m/%d")
                    }
                },
                {"_id": 0}
            )
            results.extend(list(cursor))
    else:
        if fromDate == toDate:
            cursor = reports.find(
                {
                    "date": datetime.strptime(fromDate, "%Y/%m/%d")
                },
                {"_id": 0}
            )
        else:
            cursor = reports.find(
                {
                    "date": {
                        "$gte": datetime.strptime(fromDate, "%Y/%m/%d"),
                        "$lte": datetime.strptime(toDate, "%Y/%m/%d")
                    }
                },
                {"_id": 0}
            )
        results = list(cursor)

    return jsonify(dumps(results)), 200

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
    
    if fromRequest:
        files = [
        re.sub(
            r'(\d{1,2})\.(\d{1,2})\.(\d{4})\.xls$',
            lambda m: f"{m.group(3)}/{m.group(2).zfill(2)}/{m.group(1).zfill(2)}",
            f
        )
        for f in os.listdir(REPORTS_DIRECTORY)
        if os.path.isfile(os.path.join(REPORTS_DIRECTORY, f))
        ]

        return jsonify({"reports":dumps(files)}),200
    else:
        files = [
                    re.sub(r'\.[^.]+$', '', f)  # Trgni sve posle prvata potcka
                    for f in os.listdir(REPORTS_DIRECTORY)
                    if os.path.isfile(os.path.join(REPORTS_DIRECTORY, f)) 
                ]
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
    
@app.route('/api/list_companies')
def list_companies(fromRequest = True):
    try:
        companies = db['companies']
        all_companies = companies.find({})
        company_list = []
        for document in all_companies:
            company_list.append(
                {
                    'label': document['key'].upper()+f"({document['value']})",
                    'value': document['value']
                }
            )

        if(fromRequest):
            return jsonify(dumps(company_list)), 200
        else:
            return [company['label'] for company in company_list]

    except Exception as e:
        print(f"An error occurred: {e}")
        if(fromRequest):
            return jsonify({"error": "An error occurred while fetching reports."}), 500
        else:
            return 'Грешка при влечењето на комапниите од датабазата'
def mongodb_initial():
    # Kreiranje na key-value databaza vo mongodb
    try:
        df = pd.read_csv("companies.csv")

        seen_values = set()
        list_short_name_companies = [
            item for item in (
                {"key": row[1].strip().lower(), "value": row[0]} for row in df.values
            )
            if not (item["value"] in seen_values or seen_values.add(item["value"]))
        ]

        list_all_companies = [
            item for item in (
                {"key": row[1].strip().lower(), "value": row[0]} for row in df.values
            )
        ]
        companies = db['companies']
        
        for record in list_short_name_companies:
            existing_doc = companies.find_one({"key": record["key"]})
            if not existing_doc:
                companies.insert_one(record)

        all_companies = db['all_companies']
        for record in list_all_companies:
            existing_doc = all_companies.find_one({"key": record["key"]})
            if not existing_doc:
                all_companies.insert_one(record)
        

    except FileNotFoundError:
        print("Грешка: Датотеката „companies.csv“ не е пронајдена.")
        return False

    except errors.ServerSelectionTimeoutError:
        print("Грешка: Не може да се поврзе со MongoDB. Ве молиме проверете дали серверот работи.")
        return False

    except Exception as e:
        print(f"Настана неочекувана грешка: {e}")
        return False

    return True

def start():
    if not mongodb_initial():
        sys.exit(1)
    
    app.run(host=HOST, port=PORT, debug=True, use_reloader=False)

if __name__ == '__main__':
    start()
