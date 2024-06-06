import requests
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import pytz
import time
import os
from dateutil import parser

# Database connection
def create_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='pms',
            user='pms',
            password='Welcome@123',
            auth_plugin='mysql_native_password'
        )
        if connection.is_connected():
            print("Connected to the database")
            return connection
    except Error as e:
        print(f"Error: {e}")
        return None

# Create table if not exists
def create_table(connection):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS iotData (
        id INT AUTO_INCREMENT PRIMARY KEY,
        machine VARCHAR(30) NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        pieceCount INT NOT NULL,
        stitchCount INT NOT NULL
    )
    """
    cursor = connection.cursor()
    cursor.execute(create_table_query)
    connection.commit()

# Insert shooter count into the database
def insert_shooter_count(connection, machine, piece_count, stitchCount, timestamp):
    insert_query = "INSERT INTO iotData (machine, timestamp, pieceCount, stitchCount) VALUES (%s, %s, %s, %s)"
    cursor = connection.cursor()
    cursor.execute(insert_query, (machine, timestamp, piece_count, stitchCount))
    connection.commit()

# Fetch the last shooter count for an operator
def get_last_shooter_count(connection, machine):
    select_query = "SELECT * FROM iotData WHERE machine=%s ORDER BY timestamp DESC LIMIT 1"
    cursor = connection.cursor()
    cursor.execute(select_query, (machine,))
    result = cursor.fetchone()
    return result if result else [0]*5

def get_last_stitch_count(connection, machine):
    select_query = "SELECT stitchCount FROM iotData WHERE machine=%s ORDER BY timestamp DESC LIMIT 1"
    cursor = connection.cursor()
    cursor.execute(select_query, (machine,))
    result = cursor.fetchone()
    return int(result[0]) if result else "0"

def convertTime(ts):
    ts1 = parser.parse(ts)
    ts2 = ts1 + timedelta(hours=5.5)
    ts2 = ts2.strftime('%Y-%m-%d %H:%M:%S')

    return(ts2)

# Fetch data from the API
def fetch_data_from_api():
    url = "https://utech-iiot.lk/enmonqa/public/api/MachineData_mas"
    response = requests.post(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

# Main logic to process the data and save to database
def main():
    connection = create_connection()
    if connection:
        create_table(connection)

        while True:
            data = fetch_data_from_api()
            if data:
                

                piece_count1 = data['first pullout']['data_set'].get('shooter_Count', None)
                stitchCount1 = data['first pullout']['data_set'].get('Int_Count', None)
                ts_1 = convertTime(data['first pullout'].get('time_index', None))
                
                piece_count2 = data['second pullout']['data_set'].get('shooter_Count', None)
                stitchCount2 = data['second pullout']['data_set'].get('Int_Count', None)
                ts_2 = convertTime(data['second pullout'].get('time_index', None))

                piece_count3 = data['end line checking']['data_set'].get('shooter_Count1', None)
                ts_3 = convertTime(data['end line checking'].get('time_index', None))
               

                last_shCount_p1 = get_last_shooter_count(connection, "UP007P1")[3]
                last_ts_p1 = get_last_shooter_count(connection, "UP007P1")[2]
                last_stCount_p1 = get_last_stitch_count(connection, "UP007P1")

                last_shCount_p2 = get_last_shooter_count(connection, "UP007P2")[3]
                last_ts_p2 = get_last_shooter_count(connection, "UP007P2")[2]
                last_stCount_p2 = get_last_stitch_count(connection, "UP007P2")

                last_ts_3 = get_last_shooter_count(connection, "UP007E")[2]
                last_shCount_3 = get_last_shooter_count(connection, "UP007E")[3]

              

                if (last_ts_p1 != ts_1) and (last_shCount_p1 != piece_count1 or last_stCount_p1 != stitchCount1):
                    insert_shooter_count(connection, "UP007P1",piece_count1,stitchCount1,ts_1)
                if (last_ts_p2 != ts_2) and (last_shCount_p2 != piece_count2 or last_stCount_p2 != stitchCount2):
                    insert_shooter_count(connection, "UP007P2",piece_count2,stitchCount2,ts_2)

                if (last_ts_3 != ts_3) and (last_shCount_3 != piece_count3):
                    insert_shooter_count(connection, "UP007E",piece_count3,"0",ts_3)
                
            print(f"{piece_count1},{stitchCount1},{piece_count2},{stitchCount2},{piece_count3}")
            time.sleep(60)

if __name__ == "__main__":
    main()
