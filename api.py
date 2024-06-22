from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta

app = Flask(__name__)

# MySQL database connection details
db_config = {
    'host': '4.193.94.82',
    'database': 'pms',
    'user': 'pms',
    'password': 'Welcome@123',
    'auth_plugin':'mysql_native_password'
}

def insert_data(data):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        query = """
            INSERT INTO machine_data (machineId, timeStamp, incPieceCountAccepted, incPieceCountReject, incStitchCount)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            data['machineId'], 
            data['timeStamp'].strftime("%Y-%m-%d %H:%M:%S"), 
            data['incPieceCountAccepted'], 
            data['incPieceCountReject'], 
            data['incStitchCount']
        ))
        connection.commit()
        cursor.close()
        connection.close()
        return True
    except Error as e:
        print(f"Error: {e}")
        return False

@app.route('/', methods=['POST'])
def receive_data():
    try:
        data = request.json
        
        # Check if any of the specified variables are not zero
        if (data['incPieceCountAccepted'] != 0 or 
            data['incPieceCountReject'] != 0 or 
            data['incStitchCount'] != 0):

            # Convert timeStamp to datetime and add 5 hours and 30 minutes to convert to IST
            utc_time = datetime.strptime(data['timeStamp'], "%Y-%m-%d %H:%M:%S")
            ist_time = utc_time + timedelta(hours=5, minutes=30)
            data['timeStamp'] = ist_time

            if insert_data(data):
                return jsonify({"message": "Inserted"}), 200
            else:
                return jsonify({"message": "Failea"}), 400
        else:
            return jsonify({"message": "0"}), 200
    except Exception as e:
        return jsonify({"message": f"Bad request: {str(e)}"}), 400


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=2000)
