import pandas as pd
import numpy as np
import csv
import os
from datetime import datetime
from datetime import timedelta
import transpais_logs
import time

def main():
    interval = 60 # Minutes
    while True:
        try:
            csv_filename, gx_csv_filename = transpais_logs.get_data(minutes=interval)
        except TypeError:
            print("Error conectando a DB")
            continue
        # file_name = "./logs_report.csv"

        status_logs = pd.read_csv(csv_filename)
        gx_csv = pd.read_csv(gx_csv_filename)

        status_logs["Timestamp"] = status_logs["Timestamp"].apply(lambda x: datetime.fromisoformat(x))
        status_logs["Fecha_subida"] = status_logs["Fecha_subida"].apply(lambda x: datetime.fromisoformat(x))

        critical = gx_csv.loc[(gx_csv["Eventos_pendientes"] > 1000) | (gx_csv["Status_pendientes"] > 1000)]

        # Quitar logs de batch dropping
        logs_no_dropping = status_logs.loc[status_logs["Log"].str.contains("Batch dropping").apply(lambda x: not x)]
        
        # Quitar logs que sólo sean ''}
        logs_no_dropping = logs_no_dropping.loc[(logs_no_dropping["Log"] == "{'log': ''}").apply(lambda x: not x)]
        logs_no_dropping.to_csv("no_dropping.csv")

        errors_per_unit = logs_no_dropping["Unidad"].value_counts()

        units_most_errors = pd.Series(errors_per_unit[errors_per_unit > 10])
        worst_ten_units = errors_per_unit.iloc[:10]

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
            line.append(worst_ten_units.index[n-1])
            line.append(worst_ten_units.iloc[n-1])


        with open(file_name, mode="a", encoding="utf-8") as output_file:
            writer = csv.writer(output_file)
            if not file_exists:
                writer.writeheader()

            writer.writerow(line)
            output_file.close()
    

        
        categories = pd.DataFrame(columns=["Unidad","Total", "Restarts", "Start/Reboots/Val", "SourceIDs", 
                                           "Camera connection", "Partition/Storage","Forced reboot","Others"])
        restarting_units = pd.DataFrame(columns=["Unidad", "Restarts", "Último restart"])

        for unit in units_most_errors.index:
            unit_logs = logs_no_dropping[logs_no_dropping["Unidad"] == unit]

            # Categorización de logs
            reboots = unit_logs.loc[(unit_logs["Log"] == "reboot") | (unit_logs["Log"] == "start") | (unit_logs["Log"] == "data_validation")]
            total_restarts = unit_logs.loc[unit_logs["Log"].str.contains("Restarting. Execution number:")]
            source_ids = unit_logs.loc[unit_logs["Log"].str.contains("Source id")]
            camera_connection = unit_logs.loc[unit_logs["Log"].str.contains("cameras")]
            partition = unit_logs.loc[unit_logs["Log"].str.contains("FORMATTING PARTITION") | unit_logs["Log"].str.contains("NO STORAGE DEVICE FOUND")]
            forced = unit_logs.loc[unit_logs["Log"] == "forced_reboot"]

            problems = [total_restarts, reboots, source_ids, camera_connection, partition, forced] # validation]
            sum_categories = sum([len(p) for p in problems])
            others = len(unit_logs) - sum_categories

            last_restarts = total_restarts[total_restarts["Timestamp"] > (datetime.now() - timedelta(minutes=10))]
            
            if not last_restarts.empty:
                execution_number = list(last_restarts["Log"].apply(lambda x: x.split()[4]).astype(int))[-1]
                restart_time = last_restarts.iloc[-1]["Timestamp"]
                if execution_number > 1:
                    restarting_units.loc[len(restarting_units.index)] = [unit, execution_number, restart_time.isoformat()]

    
            row = [unit, units_most_errors[unit]] + [len(p) for p in problems] + [others]
            categories.loc[len(categories.index)] = row

        print(f'\nHora: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

        print("\nUnidades con más de 10 errores en la última hora:")
        print(categories.to_string(index=False))

        print("\nUnidades con más de 1000 logs pendientes:")
        print(critical[["Unidad", "Ultima_actualizacion", "Eventos_pendientes", 
                        "Status_pendientes"]].to_string(index=False))
        
        print("\nUnidades en ciclo de restart en los últimos 10 minutos:")
        if restarting_units.empty:
            print("No hay unidades con restarts")
        else:
            print(restarting_units.to_string(index=False))
        print("\n" + "#"*80 + "\n\n")


        time.sleep(600)



if __name__ == "__main__":
    main()
