import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import csv
import os
import time

def login():
    global token

    r = requests.post('https://cmx.safe-d.aivat.io/login/',
                    data={
                        'username': 'arturo_admin',
                        'password': 'introid150325'
                    })
    
    
    if r.status_code == 200 or r.status_code == 201:
        token = r.json()["token"]
    else:
        token = None
        print(f"Login error: {r.status_code}")


def make_request(minutes=60):
    headers = {"Authorization": f"Token {token}"}
    r = requests.get("https://cmx.safe-d.aivat.io/cemex/logs/", data={"minutes": minutes}, headers=headers)
    # print(r.status_code)
    # print(r.text)
    return r, r.status_code
    
    
def main():
    try:
        login()
    except requests.exceptions.ConnectionError:
        print("Connection error")
        return

    interval = 60 # Minutos
    response, status = make_request(interval)
    if status == 401:
        login()
        response, status = make_request(interval)
    
    if status == 200 or status == 201:
        response = response.json()
    else:
        print(f"Status code: {status}")
        return
    
    with open("./output/response_cemex.json", "w") as f:
        json.dump(response, f)

    logs = response["logs"]
    devices = response["devices"]
    
    df_logs = pd.DataFrame(logs)
    df_devices = pd.DataFrame(devices)


    errors_per_unit = pd.Series([], dtype='object')
    if not df_logs.empty:
        df_logs["Timestamp"] = df_logs["Timestamp"].apply(lambda x: datetime.fromisoformat(x))
        df_logs["Fecha_subida"] = df_logs["Fecha_subida"].apply(lambda x: datetime.fromisoformat(x))
    

        logs_no_dropping = df_logs.loc[df_logs["Log"].str.contains("Batch dropping").apply(lambda x: not x)]

        aux = logs_no_dropping.loc[logs_no_dropping["Tipo"] == "Aux"]
        ignitions = logs_no_dropping.loc[logs_no_dropping["Tipo"] == "Ignición"]

        logs_no_dropping = logs_no_dropping.loc[(logs_no_dropping["Tipo"] != "Aux") & 
                                                (logs_no_dropping["Tipo"] != "Ignición")]


        errors_per_unit = logs_no_dropping["Unidad"].value_counts()


    categories_df = pd.DataFrame(columns=["Unidad", "Total", "Restarts", 
                                          "Start/Reboot/Val", "SourceIDs", "Camera connection", 
                                          "Partition/Storage", "Forced/Read only", "Others"])
    
    restarting_units = pd.DataFrame(columns=["Unidad", "Restarts", "Último restart", "Mensaje"])

    for unit in df_devices["Unidad"]:
        if unit not in errors_per_unit.index:
            row = [unit] + [0]*8
            categories_df.loc[len(categories_df.index)] = row
            continue
    
        unit_logs = logs_no_dropping[logs_no_dropping["Unidad"] == unit]
        unit_ignitions = ignitions[ignitions["Unidad"] == unit]
        aux_ignitions = aux[aux["Unidad"] == unit]

        # Categorización de logs
        conditions = {
            "restarts": unit_logs["Tipo"] == "restart",
            "reboots": unit_logs["Tipo"].isin(["reboot", "start", "data_validation"]),
            "source_ids": unit_logs["Tipo"] == "source_missing",
            "camera_connection": unit_logs["Tipo"] == "camera_missing",
            "partition": unit_logs["Tipo"] == "storage_devices",
            "forced": unit_logs["Tipo"].isin(["forced_reboot", "read_only_sdd"]),
        }

        categories = {key: unit_logs[condition] for key, condition in conditions.items()}


        # Checar qué restarts fueron justo después de una ignición (5 minutos)
        restart_times = categories["restarts"]["Timestamp"]
        ignition_times = unit_ignitions["Timestamp"]
        forgiven_restarts = set([])
        for t in restart_times.index:
            for i in ignition_times.index:
                if (ignition_times[i] + timedelta(minutes=5)) > restart_times[t] > ignition_times[i]:
                    forgiven_restarts.add(t)

        # Descartar restarts en el intervalo de 5 minutos
        categories["restarts"] = categories["restarts"].drop(forgiven_restarts)


        sum_categories = sum([len(lis) for n, lis in categories.items()])
        others = len(unit_logs) - sum_categories - len(forgiven_restarts)

        row = [unit, errors_per_unit[unit]-len(forgiven_restarts)] + [len(lis) for n, lis in categories.items()] + [others]
        categories_df.loc[len(categories_df.index)] = row

        restarts = unit_logs.loc[unit_logs["Log"].str.contains("Restarting. Execution number")]
        last_restarts = restarts[restarts["Timestamp"] > (datetime.now() - timedelta(minutes=10))]

        if not last_restarts.empty:
            execution_number = list(last_restarts["Log"].apply(lambda x: x.split()[4]).astype(int))[-1]
            restart_time = last_restarts.iloc[-1]["Timestamp"]

            if execution_number > 1:
                message = last_restarts.iloc[-1]["Log"].split("\\n\\n")[-1].split("\\n")[0].strip()
                restarting_units.loc[len(restarting_units.index)] = [unit, execution_number, 
                                                                        restart_time.isoformat(), message]

    categories_df = categories_df.sort_values(by=["Total"], ascending=False)
    output_file = "status_cemex.txt"
    with open(output_file, "w") as f:
        print("CEMEX Concreto", file=f)
        print(f'Hora: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}', file=f)        

        print("\nLogs de unidades en la última hora:", file=f)
        print(categories_df.to_string(index=False), file=f)

        print("\nUnidades con más de 1000 logs pendientes:", file=f)
        print(df_devices[["Unidad", "Ultima_actualizacion", "Jsons_eventos_pendientes", 
                        "Jsons_status_pendientes"]].to_string(index=False), file=f)
        
        print("\nUnidades en ciclo de restart en los últimos 10 minutos:", file=f)

        if restarting_units.empty:
            print("No hay unidades con restarts", file=f)
        else:
            print(restarting_units.to_string(index=False), file=f)
            
        print("\n", file=f)

    print(f'Status mandado a {output_file}')
    print(f'Hora: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n') 


    with open("./output/driving_logs.json", "w") as file:
        json.dump(response, file, ensure_ascii=False)

    
    
    
if __name__ == "__main__":
    while True:
        main()
        time.sleep(600)
