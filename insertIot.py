import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import random
import string
import time
import math
import pytz

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

def get_shift_for_line(conn,date, line_no):
    # Connect to your MySQL database

    # Create a cursor object
    cursor = conn.cursor()

    # Get the current time
    current_time = datetime.now(pytz.timezone('Asia/Colombo')).time()

    # Define the time ranges
    if current_time >= datetime.strptime('06:00:00', '%H:%M:%S').time() and current_time < datetime.strptime('14:00:00', '%H:%M:%S').time():
        time_range = '6am to 2pm'
    elif current_time >= datetime.strptime('14:00:00', '%H:%M:%S').time() and current_time < datetime.strptime('18:00:00', '%H:%M:%S').time():
        time_range = '2pm to 6pm'
    elif current_time >= datetime.strptime('14:00:00', '%H:%M:%S').time() and current_time < datetime.strptime('22:00:00', '%H:%M:%S').time():
        time_range = '2pm to 10pm'
    elif current_time >= datetime.strptime('22:00:00', '%H:%M:%S').time() or current_time < datetime.strptime('06:00:00', '%H:%M:%S').time():
        time_range = '6pm to 6am'

    # Construct the SQL query
    shift_query = """
    SELECT DISTINCT shift
    FROM dailyPlan
    WHERE date = %s AND lineno LIKE %s;
    """

    # Execute the query
    cursor.execute(shift_query, (date, line_no))

    # Fetch the results
    shifts = cursor.fetchall()

    # Close cursor and connection
    cursor.close()
    # Determine the shift based on the conditions
    if shifts:
        if any(shift[0] in ('C', 'D') for shift in shifts):
            if time_range == '6am to 2pm' or time_range == '2pm to 6pm':
                return 'C'
            else:
                return 'D'
        elif any(shift[0] in ('A', 'B') for shift in shifts):
            if time_range == '6am to 2pm':
                return 'A'
            else:
                return 'B'
    else:
        return 'No shifts found'


def get_hour_slot_and_times(shift, timestamp):
    time = timestamp.time()
    if shift == 'A':
        slots = [
            (timedelta(hours=6, minutes=0), timedelta(hours=6, minutes=20)),
            (timedelta(hours=6, minutes=20), timedelta(hours=7, minutes=20)),
            (timedelta(hours=7, minutes=20), timedelta(hours=8, minutes=20)),
            (timedelta(hours=8, minutes=20), timedelta(hours=9, minutes=20)),
            (timedelta(hours=9, minutes=20), timedelta(hours=10, minutes=20)),
            (timedelta(hours=10, minutes=20), timedelta(hours=11, minutes=20)),
            (timedelta(hours=11, minutes=20), timedelta(hours=12, minutes=20)),
            (timedelta(hours=12, minutes=20), timedelta(hours=14, minutes=0)),
        ]
    elif shift == 'B':
        slots = [
            (timedelta(hours=14, minutes=0), timedelta(hours=14, minutes=20)),
            (timedelta(hours=14, minutes=20), timedelta(hours=15, minutes=35)),
            (timedelta(hours=15, minutes=35), timedelta(hours=16, minutes=50)),
            (timedelta(hours=16, minutes=50), timedelta(hours=18, minutes=5)),
            (timedelta(hours=18, minutes=5), timedelta(hours=19, minutes=20)),
            (timedelta(hours=19, minutes=20), timedelta(hours=20, minutes=35)),
            (timedelta(hours=20, minutes=35), timedelta(hours=21, minutes=50)),
            (timedelta(hours=21, minutes=50), timedelta(hours=22, minutes=0)),
        ]
    elif shift == 'C':
        slots = [
            (timedelta(hours=6, minutes=0), timedelta(hours=6, minutes=20)),
            (timedelta(hours=6, minutes=20), timedelta(hours=7, minutes=20)),
            (timedelta(hours=7, minutes=20), timedelta(hours=8, minutes=40)),
            (timedelta(hours=8, minutes=40), timedelta(hours=10, minutes=0)),
            (timedelta(hours=10, minutes=0), timedelta(hours=11, minutes=20)),
            (timedelta(hours=11, minutes=20), timedelta(hours=12, minutes=40)),
            (timedelta(hours=12, minutes=40), timedelta(hours=14, minutes=0)),
            (timedelta(hours=14, minutes=0), timedelta(hours=15, minutes=20)),
            (timedelta(hours=15, minutes=20), timedelta(hours=16, minutes=40)),
            (timedelta(hours=16, minutes=40), timedelta(hours=17, minutes=15))
        ]
    elif shift == 'D':
        slots = [
            (timedelta(hours=18, minutes=0), timedelta(hours=18, minutes=20)),
            (timedelta(hours=18, minutes=20), timedelta(hours=19, minutes=40)),
            (timedelta(hours=19, minutes=40), timedelta(hours=21, minutes=0)),
            (timedelta(hours=21, minutes=0), timedelta(hours=22, minutes=20)),
            (timedelta(hours=22, minutes=20), timedelta(hours=23, minutes=40)),
            (timedelta(hours=23, minutes=40), timedelta(hours=23, minutes=59, seconds=59)),
            (timedelta(hours=0, minutes=0), timedelta(hours=1, minutes=20)),
            (timedelta(hours=1, minutes=20), timedelta(hours=2, minutes=40)),
            (timedelta(hours=2, minutes=40), timedelta(hours=4, minutes=0)),
            (timedelta(hours=4, minutes=0), timedelta(hours=5, minutes=20))
        ]
    
    timestamp_delta = timedelta(hours=timestamp.hour, minutes=timestamp.minute, seconds=timestamp.second)
    for i, (start, end) in enumerate(slots):
        if start <= timestamp_delta < end:
            start_time = datetime.combine(timestamp.date(), (datetime.min + start).time())
            end_time = datetime.combine(timestamp.date() if end > start else timestamp.date() + timedelta(days=1), (datetime.min + end).time())
            return i+1, start_time, end_time

    # If the timestamp doesn't align with any slot, find the closest slot
    closest_slot = min(slots, key=lambda x: abs((timestamp_delta - x[0]).total_seconds()))
    start_time = datetime.combine(timestamp.date(), (datetime.min + closest_slot[0]).time())
    end_time = datetime.combine(timestamp.date(), (datetime.min + closest_slot[1]).time())
    return slots.index(closest_slot) + 1, start_time, end_time

def ordinal(n):
    return "%d%s" % (n, "tsnrhtdd"[(n//10%10!=1)*(n%10<4)*n%10::4])

def get_piece_count(conn,timestamp, machineId, stitchCountPerPiece, shift):
    slot_number, start_time, end_time = get_hour_slot_and_times(shift, timestamp)
    if slot_number is None:
        raise ValueError("Invalid timestamp for the given shift.")

    cursor = conn.cursor()
    
    if machineId == 25:
        query = """
        SELECT SUM(incPieceCountAccepted)
        FROM machine_data
        WHERE machineId = %s
        AND timestamp >= %s
        AND timestamp < %s
        """
    else:
        query = """
        SELECT SUM(incStitchCount)
        FROM machine_data
        WHERE machineId = %s
        AND timestamp >= %s
        AND timestamp < %s
        """
    
    cursor.execute(query, (machineId, start_time, end_time))
    total_count = cursor.fetchone()[0] or 0

    cursor.close()
    if machineId == 25:
        piece_count = total_count
    else:
        piece_count = math.floor(total_count / stitchCountPerPiece)
    return piece_count, f"{ordinal(slot_number)} hour"

def get_op_piece_count(connection,hour, user_id, date, shift):
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
            SELECT userid, lineNo, Shift
            FROM operatorDailyAssignment
            WHERE date = %s AND shift = %s AND operation = %s LIMIT 1
            """

            # Execute the query
            cursor.execute(query, (date, shift, operation))
            
            # Fetch the result
            result = cursor.fetchone()
            
            # Return the userid and lineNo if a result is found
            if result:
                return result['userid'], result['lineNo'], result['Shift']
            else:
                return None, None
            
def get_adjusted_date():
                now = datetime.now(pytz.timezone('Asia/Colombo'))
                if now.hour < 6:
                    adjusted_date = now - timedelta(days=1)
                else:
                    adjusted_date = now
                return adjusted_date.date()


def main():
    conn = create_connection()
    cursor = conn.cursor()

    if conn is not None:
        try:
            
            #timestamp = '2024-06-05 08:19:00'  getMachineTS(conn,machine1)
            machine1 = 'UP007P1'
            machine2 = 'UP007P2'
            machine3 = 'UP007E'
            mId1 = 21
            mId2 = 22
            mId3 = 25
            #print(getMachineTS(conn,machine1))
            ma1_ts = datetime.now(pytz.timezone('Asia/Colombo'))#str(getMachineTS(conn,mId1))
            ma2_ts = datetime.now(pytz.timezone('Asia/Colombo'))#str(getMachineTS(conn,mId2))
            ma3_ts = datetime.now(pytz.timezone('Asia/Colombo'))#str(getMachineTS(conn,mId3))
            #print(getMachineTS(conn,machine1))
            
            date = get_adjusted_date()
            lineshift = get_shift_for_line(conn,date, 'UP007%')
            print(lineshift)
            user_id1,lineNo1,m1shift = get_userid(conn,date,lineshift,'Pullout 1')
            user_id2,lineNo2,m2shift = get_userid(conn,date,lineshift,'Pullout 2')
            user_id3,lineNo3,m3shift = get_userid(conn,date,lineshift, 'LineEnd')
            
            machine1_iot, m1hour = get_piece_count(conn, ma1_ts, mId1, 739, m1shift)
            machine2_iot, m2hour = get_piece_count(conn, ma2_ts, mId2, 712, m2shift)
            machine3_iot, m3hour = get_piece_count(conn, ma3_ts, mId3, 739, m3shift)
            print(ma1_ts,ma2_ts,ma3_ts)

            op1_pieces = get_op_piece_count(conn,m1hour, user_id1, date, m1shift)
            op2_pieces = get_op_piece_count(conn,m2hour, user_id2, date, m2shift)
            op3_pieces = get_op_piece_count(conn,m3hour, user_id3, date, m3shift)
        
            if ((machine1_iot != op1_pieces) and (machine1_iot > op1_pieces)):
                insert_piece_count(conn, user_id1, ma1_ts, 'Pullout 1',machine1_iot - op1_pieces, m1shift, m1hour, lineNo1)
                print (user_id1,'-',lineNo1,'-',m1shift,'-',m1hour,'-',machine1_iot,',',op1_pieces)

            if ((machine2_iot != op2_pieces) and (machine2_iot > op2_pieces)):
                insert_piece_count(conn, user_id2, ma2_ts, 'Pullout 2',machine2_iot - op2_pieces, m2shift, m2hour, lineNo2)
                print (user_id2,'-',lineNo2,'-',m2shift,'-',m2hour,'-',machine2_iot,',',op2_pieces)
                
            if ((machine3_iot != op3_pieces) and (machine3_iot > op3_pieces)):
                insert_piece_count(conn, user_id3, ma3_ts, 'LineEnd',machine3_iot - op3_pieces, m3shift, m3hour, lineNo3)
                print (user_id3,'-',lineNo3,'-',m3shift,'-',m3hour,'-',machine3_iot,',',op3_pieces)

        
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
