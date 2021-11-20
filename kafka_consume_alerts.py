# Notes to self : 
# Put the ARN of the topic, not the arn of the subscription
# use aws configure to update secret key and access key on your ec2 machine
# pip install boto3 - if you dont have the module already
# Don't upload online without masking the ARN

from kafka import KafkaConsumer
import sys
import json
import boto3

# AWS SNS related config
Region='us-east-1'
arn="################################"

bootstrap_servers = ['localhost:9092']
TopicName = 'DoctorQueue'
consumer = KafkaConsumer (TopicName, group_id = 'my_group_id',bootstrap_servers = bootstrap_servers,auto_offset_reset = 'earliest'

try:

    for alert in consumer:
        print("ALERT! [Paging Details....] : - %s" % (alert.value))
        client = boto3.client('sns',region_name=Region)
        response = client.publish(TargetArn=arn,Message=alert.value)

except KeyboardInterrupt:
    sys.exit(1)    