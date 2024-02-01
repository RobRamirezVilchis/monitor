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
    r = requests.get("https://tp.introid.com/logs/", data={"minutes": 60}, headers=headers)
    # print(r.status_code)
    # print(r.text)
    
    return r.json(), r.status_code


def save_worst_ten(df):
     # Guardar las 10 unidades que arrojan más errores, y cuántos son
    file_name = f"./output/most_errors.csv"
    file_exists = os.path.isfile(file_name)
    field_names = ['Inicio', 'Fin']
    fin = datetime.now().isoformat()
    inicio = (datetime.now() - timedelta(minutes=interval)).isoformat()
    line = [inicio, fin]
    for n in range(1,11):
        field_names.append(f'Unidad_{n}')
        field_names.append(f'n_errores_{n}')
        line.append(df.index[n-1])
        line.append(df.iloc[n-1])


    with open(file_name, mode="a", encoding="utf-8") as output_file:
        writer = csv.writer(output_file)
        if not file_exists:
            writer.writerow(field_names)

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
    global interval
    interval = 30
    logs = response["logs"]
    devices = response["devices"]
    
    df_logs = pd.DataFrame(logs)
    df_devices = pd.DataFrame(devices)

    df_logs["Timestamp"] = df_logs["Timestamp"].apply(lambda x: datetime.fromisoformat(x))
    df_logs["Fecha_subida"] = df_logs["Fecha_subida"].apply(lambda x: datetime.fromisoformat(x))

    critical = df_devices.loc[(df_devices["Jsons_eventos_pendientes"] > 1000) | 
                              (df_devices["Jsons_status_pendientes"] > 1000)]
    
    logs_last_hour = df_logs[df_logs["Timestamp"] > (datetime.now()-timedelta(minutes=61))]

    logs_no_dropping = logs_last_hour.loc[logs_last_hour["Log"].str.contains("Batch dropping").apply(lambda x: not x)]
        
    # Quitar logs que sólo sean ''}
    logs_no_dropping = logs_no_dropping.loc[(logs_no_dropping["Log"] == "{'log': ''}").apply(lambda x: not x)]
    logs_no_dropping.to_csv("./output/no_dropping.csv")

    aux = logs_no_dropping.loc[logs_no_dropping["Tipo"] == "Aux"]
    ignitions = logs_no_dropping.loc[logs_no_dropping["Tipo"] == "Ignición"]

    logs_no_dropping = logs_no_dropping.loc[(logs_no_dropping["Tipo"] != "Aux") & (logs_no_dropping["Tipo"] != "Ignición")]


    errors_per_unit = logs_no_dropping["Unidad"].value_counts()
    units_most_errors = pd.Series(errors_per_unit[errors_per_unit > 10])

    worst_ten_units = errors_per_unit.iloc[:10]
    save_worst_ten(worst_ten_units)

    categories_df = pd.DataFrame(columns=["Unidad","Total", "Restarts", "Start/Reboot/Val", "SourceIDs", 
                                        "Camera connection", "Partition/Storage", "Forced reboot","Others"])
    restarting_units = pd.DataFrame(columns=["Unidad", "Restarts", "Último restart"])

    for unit in errors_per_unit.index:
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
            "forced": unit_logs["Log"] == "forced_reboot"
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

        # Descartar restarts en la ventana de 5 minutos
        categories["restarts"] = categories["restarts"].drop(forgiven_restarts)


        sum_categories = sum([len(lis) for n, lis in categories.items()])
        others = len(unit_logs) - sum_categories - len(forgiven_restarts)

        if errors_per_unit[unit] > 10:
            row = [unit, units_most_errors[unit]-len(forgiven_restarts)] + [len(lis) for n, lis in categories.items()] + [others]
            categories_df.loc[len(categories_df.index)] = row

        restarts = unit_logs.loc[unit_logs["Log"].str.contains("Restarting. Execution number")]
        last_restarts = restarts[restarts["Timestamp"] > (datetime.now() - timedelta(minutes=10))]
        if not last_restarts.empty:
            execution_number = list(last_restarts["Log"].apply(lambda x: x.split()[4]).astype(int))[-1]
            restart_time = last_restarts.iloc[-1]["Timestamp"]
            if execution_number > 1:
                restarting_units.loc[len(restarting_units.index)] = [unit, execution_number, 
                                                                        restart_time.isoformat()]

    with open("status_driving.txt", "w") as f:
        print(f'\nHora: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}', file=f)        

        print("\nUnidades con más de 10 errores en la última hora:", file=f)
        print(categories_df.to_string(index=False), file=f)

        print("\nUnidades con más de 1000 logs pendientes:", file=f)
        print(critical[["Unidad", "Ultima_actualizacion", "Jsons_eventos_pendientes", 
                        "Jsons_status_pendientes"]].to_string(index=False), file=f)
        
        print("\nUnidades en ciclo de restart en los últimos 10 minutos:", file=f)
        if restarting_units.empty:
            print("No hay unidades con restarts", file=f)
        else:
            print(restarting_units.to_string(index=False), file=f)

        print("\n" + "#"*80 + "\n\n")


    with open("./output/driving_logs.json", "w") as file:
        json.dump(response, file, ensure_ascii=False)

    time.sleep(1200)
    
    
if __name__ == "__main__":
    main()