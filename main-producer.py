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
from func_support import is_new_message,db_insert,delivery_report
from pymongo import MongoClient
import socket



config = configparser.ConfigParser()
config.read("config.ini")
api_id = config['Telegram']['api_id']
api_hash = config['Telegram']['api_hash']

api_hash = str(api_hash)

phone = config['Telegram']['phone']
username = config['Telegram']['username']
channel = config['Telegram']['channel']


# Create the client and connect
client = TelegramClient(username, api_id, api_hash)


async def execute(phone,latest_message_id):
    await client.start()
    print("Client Created")
    # Ensure you're authorized
    if await client.is_user_authorized() == False:
        await client.send_code_request(phone)
        try:
            await client.sign_in(phone, input('Enter the code: '))
        except SessionPasswordNeededError:
            await client.sign_in(password=input('Password: '))




    entity = channel
    my_channel = await client.get_input_entity(entity)

    offset_id = 0
    limit = 1
    all_messages = []
    total_messages = 0
    total_count_limit = 0

    while True:
        print("Current Offset ID is:", offset_id, "; Total Messages:", total_messages)
        history = await client(GetHistoryRequest(
            peer=my_channel,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=limit,
            max_id=0,
            min_id=0,
            hash=0
        ))
        if not history.messages:
            break
        messages = history.messages
        print(messages)
        for message in messages:
            all_messages.append(message.to_dict())
        offset_id = messages[len(messages) - 1].id
        total_messages = len(all_messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break



        latest_message_id = all_messages[0]['id']
        content = all_messages[0]['message']

        return (content,latest_message_id,all_messages)







#################################################################################################################
#####ACTUAL RUNNING PART#########################################################################################

global latest_message_id
latest_message_id = 0

conf_prod = {'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()}
producer = Producer(conf_prod)

while True:
    with client:
        #get result from main function
        result = client.loop.run_until_complete(execute(phone, latest_message_id))

        #extract latest message_id, content and raw content (all_messages) from telegram
        all_messages = result[1]
        content = result[0]
        all_messages =  result[2]

        #convert msg to a string to send to producer
        msg_dict = {'mess_id':latest_message_id,'content':content}
        msg = json.dumps(msg_dict)
        print('here is the msg',msg)

        '''
        Kafka responsibility:
        After receiving message from telegram, the following block of code will use Kafka producer to push message to Kafka broker
        
        '''
        #PRODUCER PUSHING MESSAGE TO BROKER IF THERE IS NEW MESSAGE
        if is_new_message(all_messages,all_messages):
            producer.produce(
                "confluent-kp",
                msg,
                callback=lambda err, decoded_message, original_message=msg: delivery_report(  # noqa
                    err, decoded_message, original_message
                ),
            )

            producer.flush()

            time.sleep(10)
            continue
        else:
            time.sleep(10)
            continue





