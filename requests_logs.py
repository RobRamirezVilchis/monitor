import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import csv
import os
import time

def login():
    global token
    r = requests.post('https://tp.introid.com/login/',
                    data={
                        'username': 'arturo_admin',
                        'password': 'introid150325'
                    })
    
    if r.status_code == 200 or r.status_code == 201:
        token = r.json()["token"]
    else:
        token = None
        print(r.status_code)


def make_request():
    headers = {"Authorization": f"Token {token}"}
    r = requests.get("https://tp.introid.com/logs/", headers=headers)
    # print(r.status_code)
    # print(r.text)
    
    return r.json(), r.status_code


def save_worst_ten(df):
     # Guardar las 10 unidades que arrojan más errores, y cuántos son
    file_name = f"./most_errors.csv"
    file_exists = os.path.isfile(file_name)
    field_names = ['Inicio', 'Fin']
    fin = datetime.now().isoformat(timespec='seconds')
    inicio = (datetime.now() - timedelta(minutes=interval)).isoformat(timespec='seconds')
    line = [inicio, fin]
    for n in range(1,11):
        field_names.append(f'Unidad_{n}')
        field_names.append(f'n_errores_{n}')
        line.append(df.index[n-1])
        line.append(df.iloc[n-1])


    with open(file_name, mode="a", encoding="utf-8") as output_file:
        writer = csv.writer(output_file)
        if not file_exists:
            writer.writeheader()

        writer.writerow(line)
        output_file.close()
    
    
def main():
    login()
    response, status = make_request()
    if status == 401:
        login()
        response, status = make_request()
    # data = json.loads(response)
    # response keys = {"logs": [...], "devices": [...]}
    
    interval = 30
    logs = response["logs"]
    devices = response["devices"]
    
    df_logs = pd.DataFrame(logs)
    df_devices = pd.DataFrame(devices)

    df_logs["Timestamp"] = df_logs["Timestamp"].apply(lambda x: datetime.fromisoformat(x))
    df_logs["Fecha_subida"] = df_logs["Fecha_subida"].apply(lambda x: datetime.fromisoformat(x))

    critical = df_devices.loc[(df_devices["Jsons_eventos_pendientes"] > 1000) | 
                              (df_devices["Jsons_status_pendientes"] > 1000)]
    
    logs_no_dropping = df_logs.loc[df_logs["Log"].str.contains("Batch dropping").apply(lambda x: not x)]
        
    # Quitar logs que sólo sean ''}
    logs_no_dropping = logs_no_dropping.loc[(logs_no_dropping["Log"] == "{'log': ''}").apply(lambda x: not x)]
    logs_no_dropping.to_csv("no_dropping.csv")

    errors_per_unit = logs_no_dropping["Unidad"].value_counts()

    units_most_errors = pd.Series(errors_per_unit[errors_per_unit > 10])
    worst_ten_units = errors_per_unit.iloc[:10]

    save_worst_ten(worst_ten_units)

    
    categories = pd.DataFrame(columns=["Unidad","Total", "Restarts", "Start/Reboots/Val", "SourceIDs", 
                                        "Camera connection", "Partition/Storage", "Forced reboot","Others"])
    restarting_units = pd.DataFrame(columns=["Unidad", "Restarts", "Último restart"])

    for unit in units_most_errors.index:
        unit_logs = logs_no_dropping[logs_no_dropping["Unidad"] == unit]
        print(unit_logs)
        # Categorización de logs
        reboots = unit_logs.loc[(unit_logs["Log"] == "reboot") | (unit_logs["Log"] == "start") | 
                                (unit_logs["Log"] == "data_validation")]
        total_restarts = unit_logs.loc[unit_logs["Log"].str.contains("Restarting. Execution number:")]
        source_ids = unit_logs.loc[unit_logs["Log"].str.contains("Source id")]
        camera_connection = unit_logs.loc[unit_logs["Log"].str.contains("cameras")]
        partition = unit_logs.loc[unit_logs["Log"].str.contains("FORMATTING PARTITION") | 
                                  unit_logs["Log"].str.contains("NO STORAGE DEVICE FOUND")]
        forced = unit_logs.loc[unit_logs["Log"] == "forced_reboot"]

        problems = [total_restarts, reboots, source_ids, camera_connection, partition, forced] # validation]
        sum_categories = sum([len(p) for p in problems])
        others = len(unit_logs) - sum_categories

        last_restarts = total_restarts[total_restarts["Timestamp"] > (datetime.now() - timedelta(minutes=10))]
        
        if not last_restarts.empty:
            execution_number = list(last_restarts["Log"].apply(lambda x: x.split()[4]).astype(int))[-1]
            restart_time = last_restarts.iloc[-1]["Timestamp"]
            if execution_number > 1:
                restarting_units.loc[len(restarting_units.index)] = [unit, execution_number, 
                                                                     restart_time.isoformat(timespec="seconds")]


        row = [unit, units_most_errors[unit]] + [len(p) for p in problems] + [others]
        categories.loc[len(categories.index)] = row

    print("\nUnidades con más de 10 errores en la última hora:")
    print(categories.to_string(index=False))

    print("\nUnidades con más de 1000 logs pendientes:")
    print(critical[["Unidad", "Ultima_actualizacion", "Jsons_eventos_pendientes", 
                    "Jsons_status_pendientes"]].to_string(index=False))
    
    print("\nUnidades en ciclo de restart en los últimos 10 minutos:")
    if restarting_units.empty:
        print("No hay unidades con restarts")
    else:
        print(restarting_units.to_string(index=False))

    print("\n" + "#"*80 + "\n\n")

    with open("driving.json", "w") as file:
        json.dump(response, file, ensure_ascii=False)

    time.sleep(1200)
    
    
if __name__ == "__main__":
    main()