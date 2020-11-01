from confluent_kafka import Producer,Consumer,KafkaError
import configparser
import json
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.messages import (GetHistoryRequest)
from telethon.tl.types import (
    PeerChannel
)
import numpy as np
import time
import asyncio
from datetime import date, datetime
import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
from func_support import is_new_message,db_insert,delivery_report,consume
from pymongo import MongoClient
import socket
import faust
import mlflow
from pymongo import MongoClient

print('finish importing')
conf_cons = {'bootstrap.servers': 'localhost:9092',
        'group.id': "foo",
        'auto.offset.reset': 'earliest'}

try:
    consumer = Consumer(conf_cons)
    consumer.subscribe(['confluent-kp'])
    print('Consumer initialization finishes')
except:
    print('Consumer initialization got error')

try:
    client = MongoClient('localhost', 27017)
    print("Connected successfully!!!")
except:
    print("Could not connect to MongoDB")

while True:
    mess_received = consumer.poll(1.0)

    if mess_received is None:
        continue
    if mess_received.error():
        if mess_received.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(mess_received.error())
            break

    insert_mess = json.loads(mess_received.value().decode('utf-8'))

    try:
        db=client.kafka_mongo
        db.messages.insert_one(insert_mess)
        print('Records inserted into MongoDB successfully')
    except:
        print('Something wrong went during insertion')
