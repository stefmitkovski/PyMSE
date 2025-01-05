import threading
from bs4 import BeautifulSoup
import pandas as pd
import requests, os, re, time
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
# from main import db

load_dotenv()

# GLOBAL VARIBLES
MONGODB_HOST = os.getenv("HOST")
MONGODB_PORT = int(os.getenv("PORT"))
DB_NAME = os.getenv("DB")
REPORTS_URL = os.getenv("REPORTS_URL")
WEBSITE_URL = os.getenv("WEBSITE_URL")
STARTING_DATE = int(os.getenv("STARTING_DATE"))

# INITIALIZATION
download_lock = threading.Lock()
mongo_client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = mongo_client[DB_NAME]

# Funkcija za prerabotka na izvestaite(paralelno)
def processing_reports_async(report):
    file_path = 'reports/'
    companies = db['companies']
    reports = db['reports']
    priority_shares = False
    thread_name = threading.current_thread().name
    
    # Ako ne e vo opseg ripnigo
    if int(report.split('.')[2]) < STARTING_DATE:
        print(f"Nitkata {thread_name} go ripa izvestajot {report}, bidejki e nadvor od obseg")
        return
    
    # Proveri dali veke e vnesen toj zapis
    split_date = report.split('.') 
    exists = reports.find_one({'date': datetime.strptime(f"{split_date[2]}-{split_date[1]}-{split_date[0]}", "%Y-%m-%d")})
    if exists:
        print(f"Nitkata {thread_name} go ripa izvestajot {report}, bidejki e vejke e vnesen")
        return

    temp_path = file_path + report + ".xls"
    try:
        report_content = pd.read_excel(temp_path)
    except:
            # print(temp_path)
        print(f"Nitkata {thread_name} nemoze da go otvori izvestajot {report} !")
        return
        
    for _, row in report_content.iterrows():
                    
        if re.search("приоритетни акции",str(row.iloc[0]).strip()) or re.search("prioritetni akcii",str(row.iloc[0]).strip()):
            priority_shares = True
            
        if re.search("обични акции",str(row.iloc[0]).strip()) or re.search("obi~ni akcii",str(row.iloc[0]).strip()):
            priority_shares = False
                
        if pd.isna(row.iloc[1]):
            continue
            
        key = row.iloc[0].strip().lower() 
        exists = companies.find_one({"key": key})

        correct_date = f"{split_date[2]}-{split_date[1]}-{split_date[0]}"
        # print(f"Pravilen datum {correct_date}")
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
                print(f"Nitkata {thread_name} kreira nov zapis vo baza od datum: {correct_date}")
            else:
                print(f"Nitkata {thread_name} go preskoknuva izvestajot za datumot: {correct_date}")                
            print(f"POSTOI FIRMATA {row.iloc[0]}")
            print(f"INFO: {exists['value']}")
        else:
            if priority_shares:
                print(f"PIORITETNA FIRMATA {row.iloc[0]}")    
            else:
                print(f"NEPOSTOI FIRMATA {row.iloc[0]}")


def downloading_reports_async(date,current_reports):
    thread_name = threading.current_thread().name
    year, month = date
    try:
        body = {
            "cmbMonth": month,
            "cmbYear": year,
            "reportCategorySelectList": "daily-report"
        }
        result = requests.get(REPORTS_URL, data=body, timeout=10)
        result.raise_for_status()
        
        if result.status_code == 200:
            page = BeautifulSoup(result.text, "html.parser")
            daily_reports = page.find("div", id="Daily Report")
            if daily_reports:
                data = [{"href": a['href'], "text": a.get_text(strip=True)} for a in daily_reports.find_all("a", href=True)]
                for report in data:
                    with download_lock:
                        if report['text'] in current_reports:
                            print(f"Nitka {thread_name} go preskoknuva izvestajot {report['text']}, bidejki vejke postoi.")
                            return                    
                        
                    get_report = requests.get(WEBSITE_URL + report['href'])
                    if get_report.status_code == 200:
                        name = 'reports/' + report['text'] + ".xls"
                        with open(name, 'wb') as file:
                            file.write(get_report.content)
                        print(f"Nitkata {thread_name} go simna izvestajot: {report['text']}")
                    else:
                        print(f"Nitkata {thread_name} neuspea da go simne izvestajot: {report['text']}")

                print(f"Nitkata {thread_name} pravi pauza ...")
                time.sleep(3)
            else:
                print(f"Nitkata {thread_name} ne najde izestaj")
        print(f"Nitkata {thread_name} pravi pauza ...")
        time.sleep(3)
    except Exception as e:
        print(f"Nitkata {thread_name} ima problem so konekcijata: {e}")
        print(f"Nitkata {thread_name} probuva pak da se konekcira...")
        time.sleep(3)