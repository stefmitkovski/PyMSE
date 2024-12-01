from flask import Flask, jsonify
from pymongo import MongoClient, errors
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
STARTING_DATE = 2023
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


# Obrabotka na izvestai
@app.route('/api/report')
def report():
    file_path = 'ReportMK_1_20151102_20151102.xls'  # Pateka kon izestajot
    report = pd.read_excel(file_path)
    companies = db['companies']
    reports = db['reports']

    for index, row in report.iterrows():
        if pd.isna(row.iloc[1]):
            continue
        
        key = row.iloc[0].strip().lower() 
        exists = companies.find_one({"key": key})

        if exists:
            print(f"POSTOI FIRMATA {row.iloc[0]}")
            continue
        else:
            print(f"NEPOSTOI FIRMATA {row.iloc[0]}")
            
    return jsonify("ok"), 200        

# Simnigi site izvestai sto falat
@app.route('/api/report/download')
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
@app.route('/api/report/lists')
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
