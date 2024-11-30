from flask import Flask, jsonify
from pymongo import MongoClient, errors
from bs4 import BeautifulSoup
import pandas as pd
import requests, sys, os, re

app = Flask(__name__)

MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
DB_NAME = 'pyMSE'
REPORTS_DIRECTORY = 'reports'
STARTING_DATE = '2004'
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
    reports = list_reports(fromRequest=False)
    print(reports)
    if reports == None:
        print("Nema izestai\n")
        return jsonify({"ok":False}),200
    else:
        print("Gi ima slednite izestai\n")
        print(reports)
        return jsonify({"reports":reports}),200

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
