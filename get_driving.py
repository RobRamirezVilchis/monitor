from dotenv import load_dotenv
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import csv
import os
import time

def login():
    global token

    load_dotenv()
    data={
        'username': os.environ.get("TP_USERNAME"),
        'password': os.environ.get("TP_PASSWORD")
    }

    r = requests.post('https://tp.introid.com/login/',
                    data=data)
    
    
    if r.status_code == 200 or r.status_code == 201:
        token = r.json()["token"]
    else:
        token = None
        print(f"Login error: {r.status_code}")


def make_request():
    headers = {"Authorization": f"Token {token}"}
    r = requests.get("https://tp.introid.com/logs/", data={"minutes": 60}, headers=headers)
    # print(r.status_code)
    # print(r.text)
    return r, r.status_code
    
    
    
def main():
    try:
        login()
    except requests.exceptions.ConnectionError:
        print("Connection error")
        return

    response, status = make_request()
    if status == 401:
        login()
        response, status = make_request()
    
    if status == 200 or status == 201:
        response = response.json()
    else:
        print(f"Status code: {status}")
        return
    
    logs_csv = pd.DataFrame(response['logs'])
    logs_csv = logs_csv.sort_values(by=["Unidad", "Timestamp"])
    logs_csv.to_csv("./output/driving_logs.csv", index=False)
   
    
    
    
if __name__ == "__main__":
    main()
