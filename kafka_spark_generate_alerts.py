from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.conf import SparkConf
from pyspark.sql import HiveContext

sc = SparkSession  \
        .builder  \
        .appName("PVIStream")  \
        .enableHiveSupport()
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

hive_context = HiveContext(sc)

my_Schema = StructType() \
        .add("heartBeat", IntegerType()) \
        .add("bp", IntegerType()) \
        .add("customerId", IntegerType()) \
        .add("timestamp", TimestampType())

lines = sc \
        .readStream  \
        .format("parquet")  \
        .schema(my_Schema) \
        .load("hdfs://ip-10-0-0-188.ec2.internal:8022/user/ec2-user/PatientVitalInfo")

PVIinputstream = lines.selectExpr("cast(value as string)")
PVIinputstream1=PVIinputstream.select(F.json_tuple('value', 'heartBeat', 'bp', 'customerId','timestamp').alias('heartBeat', 'bp', 'customerId','timestamp'))

"""
#### CONSOLE OUTPUT
PVIIinputstream2 = PVIinputstream1  \
        .writeStream  \
        .outputMode("append")  \
        .format("console")  \
        .option("truncate", "false") \
        .start()     
"""

contacts = hive_context.table("default.patient_contacts")
patient_contacts_df = hive_context.sql("select * from patient_contacts")
joinedDF = PVIinputstream1.join(patient_contacts_df, expr(""" patientid=customerID"""), "inner")

"""

### CONSOLE OUTPUT OF JOINED DF 
query = joinedDF  \
        .writeStream  \
        .outputMode("append")  \
        .format("console")  \
        .start()
"""

threshold = hive_context.table("default.threshold_value")
threshold_df = hive_context.sql("select * from threshold_value")

df = threshold_df.join(joinedDF,"right_outer").where("alert_flag == '1'")
df1 = df.select("patientname","age","patientaddress","phone_number","admitted_ward","bp","heartBeat","timestamp","alert_message")

alert = df1 \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "52.54.92.151:9092") \
  .option("topic", "DoctorQueue") \
  .start()

alert.awaitTermination()  



