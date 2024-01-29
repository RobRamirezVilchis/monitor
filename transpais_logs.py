# python3 -m venv env
# source venv/bin/activate
# pip install pymysql
# pip install pytz

import csv
import pymysql
import datetime
import pytz
import os
from dotenv import load_dotenv

load_dotenv()
host = os.environ.get("SERVER_HOST")
user = os.environ.get("SERVER_USER")
password = os.environ.get("SERVER_PASSWORD")
db = "transpais"

def connect_to_db(host, user, password, db):
    try:
        conn = pymysql.connect(host=host, user=user, password=password, db=db, charset='utf8mb4')
        return conn
    except pymysql.MySQLError as e:
        print(f"Error connecting to MySQL Platform: {e}")
        return None

def query_db(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        column_names = [desc[0] for desc in cursor.description]
        return cursor.fetchall(), column_names
    
def save_to_csv(data, filename, header=None):
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        if not header and data:
            header = data[0].keys()

        writer = csv.DictWriter(file, fieldnames=header)

        writer.writeheader()
        for row in data:
            writer.writerow(row)

def get_data(minutes=10):
    
    current_date = datetime.datetime.now(tz=pytz.timezone("America/Monterrey")).replace(tzinfo=pytz.utc)
    range_date = current_date - datetime.timedelta(minutes=minutes)
    
    initial_date = range_date.strftime("%Y-%m-%d")
    initial_datetime = range_date.strftime("%Y-%m-%d %H:%M:%S")
    initial_datetime_name = range_date.strftime("%Y-%m-%dT%H:%M:%S")
    final_date = current_date.strftime("%Y-%m-%d")
    final_datetime = current_date.strftime("%Y-%m-%d %H:%M:%S")
    final_datetime_name = current_date.strftime("%Y-%m-%dT%H:%M:%S")

    conn = connect_to_db(host, user, password, db)
    if conn is None:
        return

    try:
        query = "SELECT transpais_gx_log.*, transpais_gx.*, transpais_camiones.* " + \
        "FROM transpais_gx_log " + \
        "INNER JOIN transpais_gx ON transpais_gx_log.gx_id = transpais_gx.id " + \
        "INNER JOIN transpais_camiones ON transpais_gx.id_camion_id = transpais_camiones.id " + \
        f"WHERE transpais_gx_log.upload_datetime >= '{initial_datetime}' " + \
        f"AND transpais_gx_log.upload_datetime <= '{final_datetime}' " + \
        f"AND transpais_gx_log.upload_date >= '{initial_date}' " + \
        f"AND transpais_gx_log.upload_date <= '{final_date}' " + \
        "AND transpais_gx_log.message NOT IN ('Event', 'Coords', 'E/S', 'E/S Temp') " + \
        "ORDER BY transpais_camiones.descriptor, transpais_gx_log.upload_datetime;"
        response, columns = query_db(conn, query)

        data = []
        unit_index = columns.index("descriptor")
        for row in response:
            data.append({
                "Unidad": row[unit_index],
                "Fecha_subida": row[8].strftime("%Y-%m-%d %H:%M:%S") if row[8] is not None else "null",
                "Timestamp": row[3].replace(tzinfo=pytz.utc).astimezone(pytz.timezone("America/Monterrey")).strftime("%Y-%m-%d %H:%M:%S"),
                "Error": "True" if not row[1] and not row[5] else "False",
                "Log": row[2],
            })

        # csv_filename = f"transpais_logs_{initial_datetime_name}_{final_datetime_name}.csv"
        csv_filename = f"/home/spare/Documents/script/output/transpais_logs.csv"
        save_to_csv(data, csv_filename)
        print(f"Data has been written to {csv_filename}")
        
        gx_query = "SELECT * FROM transpais_gx WHERE transpais_gx.status = 1 ORDER BY transpais_gx.description;"
        response_gx, columns_gx = query_db(conn, gx_query)
        
        last_activity_index = columns_gx.index("last_activity")
        pending_events_index = columns_gx.index("pending_event_jsons")
        pending_status_index = columns_gx.index("pending_status_jsons")
        last_date_pending_event_index = columns_gx.index("last_date_pending_event")
        last_date_pending_status_index = columns_gx.index("last_date_pending_status")
        data_gx = []
        for row in response_gx:
            data_gx.append({
                "Unidad": row[1],
                "Ultima_actualizacion": row[last_activity_index].strftime("%Y-%m-%d %H:%M:%S") if row[last_activity_index] is not None else "null",
                "Eventos_pendientes": row[pending_events_index],
                "Status_pendientes": row[pending_status_index],
                "Actualizacion_json_eventos": row[last_date_pending_event_index].strftime("%Y-%m-%d %H:%M:%S") if row[last_date_pending_event_index] is not None else "null",
                "Actualizacion_json_status": row[last_date_pending_status_index].strftime("%Y-%m-%d %H:%M:%S") if row[last_date_pending_status_index] is not None else "null",
            })
            
        # csv_filename_gx = f"./transpais_gx_{initial_datetime_name}_{final_datetime_name}.csv"
        csv_filename_gx = f"/home/spare/Documents/script/output/transpais_gx.csv"
        save_to_csv(data_gx, csv_filename_gx)
        print(f"Data has been written to {csv_filename_gx}")
        
    finally:
        conn.close()

    return csv_filename, csv_filename_gx

if __name__ == "__main__":
    get_data()
