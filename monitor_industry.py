import os
from dotenv import load_dotenv
import requests
import json
import time
from datetime import datetime, timedelta

clients = ["trm", "rgs", "bkrt"]
client_names = {
    "trm": "Ternium",
    "rgs": "Ragasa",
    "bkrt": "Bekaert"
}


def get_credentials(clients):
    load_dotenv()
    credentials = {}
    for client in clients:
        credentials[client] = {
            "username": os.environ.get(f'{client.upper()}_USERNAME'),
            "password": os.environ.get(f'{client.upper()}_PASSWORD')
        }
    
    return credentials
    

def get_data(client, credentials, hours=1, minutes=0):
    login_url = f'https://{client}.industry.aivat.io/login/'
    status_url = f'https://{client}.industry.aivat.io/stats_json/'
    
    response = requests.post(login_url, data=credentials[client]).text
    token = json.loads(response)['token']

    time_interval = {
        "initial_datetime": (datetime.now() - timedelta(hours=hours, minutes=minutes)).isoformat(timespec="seconds")
    }

    response = requests.get(status_url, headers={"Authorization": f'Token {token}'}, data=time_interval).json()
    return response


def main():
    while True:
        credentials = get_credentials(clients)
        print(f'Hora: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        for client in clients:
            delay_found = False
            try:
                response = get_data(client, credentials)
            except:
                print("Error conectando a DB")
                continue

            for device, logs in response.items():
                last_register_time = datetime.fromisoformat(logs[0]["register_time"][:-1])
                time_since_log = datetime.now() - (last_register_time - timedelta(hours=6))
                if time_since_log > timedelta(minutes=11):
                    delay = str(time_since_log-timedelta(minutes=10)).split('.')[0]
                    print(f'{client_names[client]} {device} delayed by {delay}')
                    delay_found = True

                log = logs[0]["log"]
                if not log == "":
                    print(f"{client_names[client]} {device} log: {log}")

            if not delay_found:
                print(f'{client_names[client]} sin retrasos')

        print("\n")
        time.sleep(120)

if __name__ == "__main__":
    main()
