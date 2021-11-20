# Import Dependencies
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import json

# Set topic name and kafka server address/port

topic="PatientVitalInfo"
kafka_server="52.54.92.151:9092"

spark = SparkSession  \
        .builder  \
        .appName("StructuredSocketRead")  \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

lines = spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers",kafka_server)  \
        .option("subscribe",topic)  \
        .load()


my_Schema = StructType() \
        .add("heartBeat", IntegerType()) \
        .add("bp", IntegerType()) \
        .add("customerId", IntegerType()) \
        .add("timestamp", TimestampType())


kafka_raw_input_DF = lines.selectExpr("cast(value as string)","cast(timestamp as string)")
kafka_raw_input=kafka_raw_input_DF.select('timestamp', F.json_tuple('value', 'heartBeat', 'bp', 'customerId').alias('heartBeat', 'bp', 'customerId'))

KafkaQuery = kafka_raw_input  \
        .writeStream  \
        .outputMode("append")  \
        .format("console")  \
        .option("truncate", "false") \
        .start()


#write streaming data to parquet file
write_parquet_df = kafka_raw_input.writeStream.outputMode("Append") \
.format("parquet") \
.option("format","append") \
.option("path","PatientVitalInfo") \
.option("checkpointLocation", "PatientVitalInfo_parquet") \
.trigger(processingTime="1 minute") \
.start()
write_parquet_df.awaitTermination()