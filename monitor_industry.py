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
    
    if os.path.isfile("delays.json"):
        with open("delays.json", "r") as file:
            recent_delays = json.loads(file)
    else:
        recent_delays = {c: [] for c in clients}

    last_delay = {c: datetime.min for c in clients}

    
    while True:
        all_responses = {c: {} for c in clients}
        credentials = get_credentials(clients)
        print(f'Hora: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

        for client in clients:
            delay_found = False
            try:
                response = get_data(client, credentials)
            except:
                print("Error conectando a DB")
                continue
            all_responses[client] = response

            if response == {}:
                print(f"{client_names[client]} sin conexión")
                continue


            for device, logs in response.items():
                
                last_register_time = datetime.fromisoformat(logs[0]["register_time"][:-1])
                penul_register_time = datetime.fromisoformat(logs[1]["register_time"][:-1])
                time_since_log = datetime.now() - (last_register_time - timedelta(hours=6))
                prev_time_gap = last_register_time - penul_register_time

                # Si hay un restraso ahorita
                if time_since_log > timedelta(minutes=11):
                    delay = str(time_since_log-timedelta(minutes=10)).split('.')[0]
                    print(f'{client_names[client]} {device} atrasado por {delay}')
                    delay_found = True

                    # Si el delay no está registrado aún, registrarlo
                    if recent_delays[client] == [] or recent_delays[client][-1] != last_register_time.isoformat():
                        recent_delays[client].append(last_register_time.isoformat())

                    last_delay[client] = datetime.now()

                # Si hubo un retraso entre el último y el penúltimo
                elif prev_time_gap > timedelta(minutes=11):
                    # Si el delay no está registrado aún, registrarlo
                    if recent_delays[client] == [] or recent_delays[client][-1] != penul_register_time.isoformat():
                        recent_delays[client].append(penul_register_time.isoformat())
                        delay = str((prev_time_gap - timedelta(minutes=10))).split('.')[0]
                        print(f'{client_names[client]} {device} se atrasó por {delay}')
                        delay_found = True

                                
                log = logs[0]["log"]
                if not log == "":
                    print(f"{client_names[client]} {device} log: {log}")
            
            recent_delays[client] = [r for r in recent_delays[client] 
                                     if (datetime.now() - datetime.fromisoformat(r)) < timedelta(hours=24)]

            with open("delays.json", "w") as file:
                json.dump(recent_delays, file, ensure_ascii=False)

            if not delay_found:
                print(f'{client_names[client]} sin retrasos ({len(recent_delays[client])} en las últimas 24h)')

        with open("industry_logs.json", "w") as file:
            json.dump(all_responses, file, ensure_ascii=False)

        print("\n")
        time.sleep(600)


if __name__ == "__main__":
    main()
