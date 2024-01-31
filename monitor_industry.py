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

    response = requests.get(status_url, headers={"Authorization": f'Token {token}'}, data=time_interval)
    return response.json()


def main():
    
    if os.path.isdir("delays.json"):
        with open("delays.json", "r") as file:
            recent_delays = json.loads(file)
    else:
        recent_delays = {c: [] for c in clients}

    last_delay = {c: datetime.min for c in clients}

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

            if response == {}:
                print(f"{client_names[client]} sin conexión")
                continue
            

            for device, logs in response.items():
                
                last_register_time = datetime.fromisoformat(logs[0]["register_time"][:-1])
                time_since_log = datetime.now() - (last_register_time - timedelta(hours=6))
                
                if time_since_log > timedelta(minutes=11):
                    delay = str(time_since_log-timedelta(minutes=10)).split('.')[0]
                    print(f'{client_names[client]} {device} atrasado por {delay}')
                    delay_found = True

                    
                    if (datetime.now() - last_delay[client]) > timedelta(minutes=11):
                        recent_delays[client].append(datetime.now().isoformat())

                    last_delay[client] = datetime.now()
                                
                log = logs[0]["log"]
                if not log == "":
                    print(f"{client_names[client]} {device} log: {log}")
            
            recent_delays[client] = [r for r in recent_delays[client] 
                                     if (datetime.now() - datetime.fromisoformat(r)) < timedelta(hours=24)]
            
            with open("delays.json", "w") as file:
                json.dump(recent_delays, file, ensure_ascii=False)

            if not delay_found:
                print(f'{client_names[client]} sin retrasos ({len(recent_delays[client])} en las últimas 24h)')

        print("\n")
        time.sleep(600)

if __name__ == "__main__":
    main()
