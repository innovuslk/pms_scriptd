import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import random
import string
import time

# Connect to your SQLite database
def create_connection():
    try:
        connection = mysql.connector.connect(
            host='4.193.94.82',
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


def get_shift_and_hour(timestamp):
    shift_A_hours = {
        1: ('06:00', '06:20'),
        2: ('06:20', '07:40'),
        3: ('07:40', '08:40'),
        4: ('08:40', '09:40'),
        5: ('09:40', '10:40'),
        6: ('10:40', '12:00'),
        7: ('12:00', '13:00'),
        8: ('13:00', '14:00')
        #9: ('14:00', '15:00'),  
        #10: ('15:00', '16:00'),  
        #11: ('16:00', '17:30')  
    }

    shift_B_hours = {
        1: ('14:00', '15:00'),  
        2: ('15:00', '16:00'),  
        3: ('16:00', '17:00'), 
        4: ('17:00', '18:00'),  # Start of shift B
        5: ('18:00', '19:00'),  # Adjusted for 11 hours
        6: ('19:00', '20:15'),  # Adjusted for 11 hours
        7: ('20:15', '21:15'),  # Adjusted for 11 hours
        8: ('21:15', '22:00')  # Adjusted for 11 hours
        #6: ('23:15', '00:35'),  # Adjusted for 11 hours, next day
        #7: ('00:35', '01:35'),  # Adjusted for 11 hours, next day
        #8: ('01:35', '02:40'),  # Adjusted for 11 hours, next day
        #9: ('02:40', '03:45'),  # Adjusted for 11 hours, next day
        #10: ('03:45', '05:00'),  # Adjusted for 11 hours, next day
        #11: ('05:00', '05:30')  # End of shift B, next day
    }

    ordinal_hours = {
        1: '1',
        2: '2',
        3: '3',
        4: '4',
        5: '5',
        6: '6',
        7: '7',
        8: '8',
        9: '9',
        10: '10',
        11: '11'
    }
    
    
    timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    time_of_day = timestamp.time()
    
    for hour, (start, end) in shift_A_hours.items():
        if time_of_day >= datetime.strptime(start, '%H:%M').time() and time_of_day < datetime.strptime(end, '%H:%M').time():
            return 'A', ordinal_hours[hour]
    
    for hour, (start, end) in shift_B_hours.items():
        if time_of_day >= datetime.strptime(start, '%H:%M').time() and time_of_day < datetime.strptime(end, '%H:%M').time():
            return 'B', ordinal_hours[hour]
    
    return None, None  # If the timestamp doesn't fall within any shift hours

def getMachineTS(conn,machine):
    select_query = "SELECT timestamp FROM iotData WHERE machine=%s ORDER BY timestamp DESC LIMIT 1"
    cursor = conn.cursor()
    cursor.execute(select_query, (machine,))
    result = cursor.fetchone()
    return result[0]

def get_cumulative_piece_count(connection,machine, timestamp):
    shift, hour_str = get_shift_and_hour(timestamp)
    if shift is None or hour_str is None:
        return 0

    # Define shift hours inside the function
    shift_A_hours = {
        1: ('06:00', '06:20'),
        2: ('06:20', '07:40'),
        3: ('07:40', '08:40'),
        4: ('08:40', '09:40'),
        5: ('09:40', '10:40'),
        6: ('10:40', '12:00'),
        7: ('12:00', '13:00'),
        8: ('13:00', '14:00')
       # 9: ('14:00', '15:00'),  
        #10: ('15:00', '16:00'),  
        #11: ('16:00', '17:30')  
    }

    shift_B_hours = {
        1: ('14:00', '15:00'),  
        2: ('15:00', '16:00'),  
        3: ('16:00', '17:00'), 
        4: ('17:00', '18:00'),  # Start of shift B
        5: ('18:00', '19:00'),  # Adjusted for 11 hours
        6: ('19:00', '20:15'),  # Adjusted for 11 hours
        7: ('20:15', '21:15'),  # Adjusted for 11 hours
        8: ('21:15', '22:00')  # Adjusted for 11 hours
       # 9: ('02:40', '03:45'),  # Adjusted for 11 hours, next day
        #10: ('03:45', '05:00'),  # Adjusted for 11 hours, next day
        #11: ('05:00', '05:30')  # End of shift B, next day
    }

    # Define mapping for hour strings to integers
    hour_mapping = {
        '1': 1,
        '2': 2,
        '3': 3,
        '4': 4,
        '5': 5,
        '6': 6,
        '7': 7,
        '8': 8,
        '9': 9,
        '10': 10,
        '11': 11
    }


    hour = hour_mapping[hour_str]
    
    start_time, end_time = None, None
    
    if shift == 'A':
        start_time, end_time = shift_A_hours[hour]
    elif shift == 'B':
        start_time, end_time = shift_B_hours[hour]
    
    start_datetime = datetime.strptime(f"{timestamp[:10]} {start_time}", "%Y-%m-%d %H:%M")
    end_datetime = datetime.strptime(f"{timestamp[:10]} {end_time}", "%Y-%m-%d %H:%M")
    
    cursor = connection.cursor(dictionary=True)
    
    query = """
    SELECT pieceCount FROM iotData
    WHERE machine = %s AND timestamp BETWEEN %s AND %s
    ORDER BY timestamp
    """
    cursor.execute(query, (machine, start_datetime, end_datetime))
    results = cursor.fetchall()
    
    cursor.close()
    
    if results:
        start_piece_count = results[0]['pieceCount']
        end_piece_count = results[-1]['pieceCount']
        return end_piece_count - start_piece_count
    else:
        return 0

def get_piece_count(connection,hour, user_id, date, shift):
    cursor = connection.cursor(dictionary=True)

    query = """
    SELECT SUM(pieceCount) as piececount FROM pieceCount
    WHERE hour = %s AND userid = %s AND timestamp > %s AND shift = %s
    """
    cursor.execute(query, (hour, user_id, date, shift))
    result = cursor.fetchone()
    
    cursor.close()
    
    if result and result['piececount'] is not None:
        return result['piececount']
    else:
        return 0

def generate_random_string(length=9):
    characters = string.ascii_letters + string.digits  # This includes both uppercase and lowercase letters, and digits
    random_string = ''.join(random.choice(characters) for _ in range(length))
    return random_string


def insert_piece_count(connection, userid, timestamp, operator ,pieceCount, shift, slot, lineNo):
    insert_query = "INSERT INTO pieceCount (id, userid, timestamp, salesOrder, lineItem, operation, plantName, pieceCount, shift, hour, lineNo) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor = connection.cursor()
    cursor.execute(insert_query, (generate_random_string(), userid, timestamp, 'R623-745(R1)', '10',operator, 'UPLP', pieceCount, shift, slot, lineNo))
    connection.commit()


def get_userid(connection,date, shift, operation):

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)

            # Define the query
            query = """
            SELECT userid, lineNo
            FROM operatorDailyAssignment
            WHERE date = %s AND shift = %s AND operation = %s LIMIT 1
            """

            # Execute the query
            cursor.execute(query, (date, shift, operation))
            
            # Fetch the result
            result = cursor.fetchone()
            
            # Return the userid and lineNo if a result is found
            if result:
                return result['userid'], result['lineNo']
            else:
                return None, None


def main():
    conn = create_connection()
    cursor = conn.cursor()

    if conn is not None:
        try:
            
            #timestamp = '2024-06-05 08:19:00'  getMachineTS(conn,machine1)
            machine1 = 'UP007P1'
            machine2 = 'UP007P2'
            machine3 = 'UP007E'
            ma1_ts = str(getMachineTS(conn,machine1))
            ma2_ts = str(getMachineTS(conn,machine2))
            ma3_ts = str(getMachineTS(conn,machine3))
            print(getMachineTS(conn,machine1))

            # Determine shift and hour
            m1shift, m1hour = get_shift_and_hour(ma1_ts)
            m2shift, m2hour = get_shift_and_hour(ma2_ts)
            m3shift, m3hour = get_shift_and_hour(ma3_ts)

            #print(f"Shift: {shift}, Hour: {hour}")

            # Assuming user ID 101 and date '2024-06-03'

            date = datetime.now().strftime('%Y-%m-%d')
            print(date)
            user_id1,lineNo1 = get_userid(conn,date,m1shift,'Pullout 1')
            user_id2,lineNo2 = get_userid(conn,date,m2shift,'Pullout 2')
            user_id3,lineNo3 = get_userid(conn,date,m3shift,'LineEnd')

            machine1_iot = get_cumulative_piece_count(conn,machine1, ma1_ts)
            machine2_iot = get_cumulative_piece_count(conn,machine2, ma2_ts)
            machine3_iot = get_cumulative_piece_count(conn,machine3, ma3_ts)

            op1_pieces = get_piece_count(conn,m1hour, get_userid(conn,date,m1shift,'Pullout 1')[0], date, m1shift)
            op2_pieces = get_piece_count(conn,m2hour, get_userid(conn,date,m2shift,'Pullout 2')[0], date, m2shift)
            op3_pieces = get_piece_count(conn,m3hour, get_userid(conn,date,m3shift,'LineEnd')[0], date, m3shift)


            if ((machine1_iot != op1_pieces) and (machine1_iot > op1_pieces)):
                insert_piece_count(conn, user_id1, ma1_ts, 'Pullout 1',machine1_iot - op1_pieces, m1shift, m1hour, lineNo1)
                print(machine1_iot,op1_pieces,lineNo1,'Pullout 1')

            if ((machine2_iot != op2_pieces) and (machine2_iot > op2_pieces)):
                insert_piece_count(conn, user_id2, ma2_ts, 'Pullout 2',machine2_iot - op2_pieces, m2shift, m2hour, lineNo2)
                print(machine2_iot,op2_pieces,lineNo2,'Pullout 2')

            if ((machine3_iot != op3_pieces) and (machine3_iot > op3_pieces)):
                insert_piece_count(conn, user_id3, ma3_ts, 'LineEnd',machine3_iot - op3_pieces, m3shift, m3hour, lineNo3)
                print(machine3_iot,op3_pieces,lineNo3,'LineEnd')
        except Error as e:
            print(f"Error during database operations: {e}")

        finally:
            # Ensure the cursor and connection are closed
            if conn.is_connected():
                cursor.close()
                conn.close()
                print("MySQL connection is closed")

while True:
    try:
        main()
    except Exception as e:
        print(f"An error occurred: {e}")
    
    # Wait for 1 minute (60 seconds) before running again
    time.sleep(30)
