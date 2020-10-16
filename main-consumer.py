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

conf_cons = {'bootstrap.servers': 'localhost:9092',
        'group.id': "foo",
        'auto.offset.reset': 'earliest'}

consumer = Consumer(conf_cons)
consumer.subscribe(['confluent-kp'])

client = MongoClient('localhost', 27017)

while True:
    mess_received = consume(consumer,1)


    print('Received message: {}'.format(mess_received.value().decode('utf-8')))

    print(type(json.loads(mess_received.value().decode('utf-8'))))


    # db=client.kafka_mongo
    # db.messages.insert_one(mess_received)
