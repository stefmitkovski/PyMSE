from flask import Flask, jsonify, request
from pymongo import MongoClient, errors
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import pandas as pd
import requests, sys, os, re, time
from datetime import datetime, timedelta

app = Flask(__name__)

# DATABASE CREDENTIALS
MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
DB_NAME = 'pyMSE'

# OTHER GLOBAL VARIBLES
REPORTS_URL = "https://www.mse.mk/mk/reports"
WEBSITE_URL = "https://www.mse.mk/"
REPORTS_DIRECTORY = 'reports'
STARTING_DATE = 2020
# COLLECTION_NAME = 'reports'

mongo_client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = mongo_client[DB_NAME]
# collection = db[COLLECTION_NAME]

# Zemanje na najnovi informacii na momentalno trguvani akcii
@app.route('/api/latest')
def latest():
    collection = db['reports']
    try:
        url = "https://www.mse.mk/en/"
        result = requests.get(url, timeout=10)
        result.raise_for_status()
        page = BeautifulSoup(result.text, "html.parser")
        sidebar = page.find("ul", {"class": "newsticker"})
        if not sidebar:
            return jsonify({"error": "Sidebar not found"}), 404
        
        items = sidebar.find_all("li")
        ret = []
        for item in items:
            if item.span:
                arr = str(item.span.text).split()
                if len(arr) >= 3:
                    ret.append({
                        "symbol": arr[0],
                        "value": arr[1],
                        "percentage": arr[2]
                    })
        
        if not ret:
            return jsonify({"error": "No valid data found"}), 404

        collection.replace_one({"key": "latest"}, {"key": "latest", "value": ret}, upsert=True)

        return jsonify(ret), 200

    except requests.RequestException as e:
        return jsonify({"error": f"Failed to fetch URL: {str(e)}"}), 500
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


# API endpoint za zapocnuvanjeto na prerabotkata na izvestaite
# i za vrakanje na informaciija za odredena firma
@app.route('/api/reports', methods=['GET','POST'])
def report():
    if request.method == 'GET':
        print('Zapocnuvanje na prerabotkata na izevstaite')
        try:
            list_of_reports = list_reports(fromRequest=False)
            print(list_of_reports)
            with ThreadPoolExecutor(max_workers=50) as executor:
                executor.map(processing_reports,list_of_reports)
            
            return jsonify({"ok":True}),200
        except:
            print("Greska pri prevzemanje na izvestaite")
            return jsonify({"error":True}),200
    
    elif request.method == 'POST':
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

# Funkcija za prerabotka na izvestaite(paralelno)
def processing_reports(report):
    file_path = 'reports/'
    companies = db['companies']
    reports = db['reports']
    priority_shares = False
    
    # Ako ne e vo opseg ripnigo
    if int(report.split('.')[2]) < STARTING_DATE:
        print(f"Se ripnuva izvestajot za datumot {report}")
        return
    
    # for report in list_of_reports:
    temp_path = file_path + report + ".xls"
    try:
        report_content = pd.read_excel(temp_path)
    except:
            # print(temp_path)
        print(f"Nemoze da se pristapi izvestajot {report}")
        return
        
    for index, row in report_content.iterrows():
                    
        if re.search("приоритетни акции",str(row.iloc[0]).strip()) or re.search("prioritetni akcii",str(row.iloc[0]).strip()):
            priority_shares = True
            
        if re.search("обични акции",str(row.iloc[0]).strip()) or re.search("obi~ni akcii",str(row.iloc[0]).strip()):
            priority_shares = False
                
        if pd.isna(row.iloc[1]):
            continue
            
        key = row.iloc[0].strip().lower() 
        exists = companies.find_one({"key": key})

        split_date = report.split('.') 
        correct_date = f"{split_date[2]}-{split_date[1]}-{split_date[0]}"
        print(f"Pravilen datum {correct_date}")
        if exists and not priority_shares:
            record = {
                    "symbol": exists['value'],
                    "date": datetime.strptime(correct_date, "%Y-%m-%d"),
                    "average_price": row.iloc[1],
                    "change": row.iloc[2],
                    "purchase_price": row.iloc[3],
                    "sale_price": row.iloc[4],
                    "max": row.iloc[5],
                    "min": row.iloc[6],
                    "last_price": row.iloc[7],
                    "quantity": row.iloc[8],
                    "turnover_in_1000_den": row.iloc[9],
                }
                
            if not reports.find_one({"date": report, "symbol": exists['value']}):
                reports.insert_one(record)
                print("Record inserted.")
            # else:
            #     print("Record already exists. Skipping insertion.")                
            # print(f"POSTOI FIRMATA {row.iloc[0]}")
            # print(f"INFO: {exists['value']}")
        # else:
        #     if priority_shares:
        #         print(f"PIORITETNA FIRMATA {row.iloc[0]}")    
        #     else:
        #         print(f"NEPOSTOI FIRMATA {row.iloc[0]}")
            
    # return jsonify("ok"), 200        

# Simnigi site izvestai sto falat
@app.route('/api/reports/download')
def download():
    current_reports = list_reports(fromRequest=False)    

    start_date = datetime(STARTING_DATE, 1, 1)
    current_date = datetime.now()

    current = start_date
    while current <= current_date:
        date = current.strftime("%Y-%m").split("-")
        print(f"Godina: {date[0]}, mesec: {date[1]}")

        try:
            body = {
                        "cmbMonth": date[1],
                        "cmbYear": date[0],
                        "reportCategorySelectList":"daily-report"
                    }
            result = requests.get(REPORTS_URL, data=body, timeout=10)
            result.raise_for_status()
            
            if result.status_code == 200:
                page = BeautifulSoup(result.text, "html.parser")
                daily_reports = page.find("div", id="Daily Report")
                if daily_reports:
                    data = [{"href": a['href'], "text": a.get_text(strip=True)} for a in daily_reports.find_all("a", href=True)]
                    for report in data:
                        if report['text'] in current_reports:
                            print(f"Go ima veke izvestajot {report['text']}, preskoknuvanje...")
                        else:
                            get_report = requests.get(WEBSITE_URL+report['href'])
                            if get_report.status_code == 200:
                                name = 'reports/'+report['text']+".xls"
                                with open(name,'wb') as file:
                                    file.write(get_report.content)
                                print(f"Uspesno zacuvan izvestajot: {report['text']}")
                            else:
                                print(f"Neuspesno prevzemanje na izvestajot: {report['text']}")

                            print("Cekanje...")
                            time.sleep(3)
                else:
                    print("Ne e najden takov div")
            print("Cekanje...")
            time.sleep(3)
        except:
            print("Nema konekcija so stranata")
            print("Cekanje...")
            time.sleep(3)
            continue # Obidi se povtorno da stapis vo konekcija so stranata
        
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

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
@app.route('/api/list_reports')
def func_list_reports():
    file_path = 'reports/'
    
    list_of_reports = list_reports(fromRequest=False)
    list_companies = []
    
    for report in list_of_reports:
        temp_path = file_path + report + ".xls"

        try:
            report_content = pd.read_excel(temp_path)
        except:
            print(f"Nemoze da se pristapi izvestajot {report}")
            continue

        for index, row in report_content.iterrows():

            if pd.isna(row.iloc[1]):
                continue

            company_name = str(row.iloc[0]).strip()
            if company_name not in list_companies:
                list_companies.append(company_name)
                
    return jsonify(list_companies), 200
    
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
