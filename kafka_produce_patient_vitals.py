from kafka import KafkaProducer
import mysql.connector
import json
import time


connection = mysql.connector.connect(host='upgraddetest.cyaielc9bmnf.us-east-1.rds.amazonaws.com', database='testdatabase',user='student', password='STUDENT123')
cursor=connection.cursor()
statement='SELECT * FROM patients_vital_info'
cursor.execute(statement)
query_data=cursor.fetchall()

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

pvi_dict = {}

for i in query_data:
    pvi_dict['customerId']=i[0]
    pvi_dict['heartBeat']=i[1]
    pvi_dict['bp']=i[2]
    my_dict=json.dumps(pvi_dict)
    producer.send('PatientVitalInfo',str(my_dict)) # To Kafka
    print(my_dict) # Console Output
    time.sleep(1) 