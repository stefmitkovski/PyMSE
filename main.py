from flask import Flask, jsonify
from pymongo import MongoClient
from bs4 import BeautifulSoup
import requests

app = Flask(__name__)

MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
DB_NAME = 'pyMSE'
COLLECTION_NAME = 'reports'

mongo_client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]

@app.route('/api/latest')
def latest():
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

# @app.route('/submit', methods=['POST'])
# def submit():
#     name = request.form.get('name')
#     message = request.form.get('message')
    
#     return f'Hello {name}, you sent the message: "{message}"'

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5000,debug=True)
