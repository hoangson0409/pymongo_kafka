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
from django.core.serializers.json import DjangoJSONEncoder



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

#############################################################
#TESTING
from agent import adding,Add,send_value

async def multitask(task1, task2):
    res = await asyncio.gather(task1,task2,return_exceptions=True)
    return res


while True:
    with client:
        #get result from main function
        result,res2 = client.loop.run_until_complete(multitask(execute(phone, latest_message_id),send_value()))
        print('here is the type of clien loop: ', type(client.loop))


        #extract latest message_id, content and raw content (all_messages) from telegram
        #latest_message_id = result[1]
        content = result[0]
        all_messages =  result[2]
        all_messages[0]['is_new_mess'] = False

        '''
        Kafka responsibility:
        After receiving message from telegram, the following block of code will use Kafka producer to push message to Kafka broker
        
        '''
        #PRODUCER PUSHING MESSAGE TO BROKER IF THERE IS NEW MESSAGE
        if is_new_message(all_messages,latest_message_id):

            all_messages[0]['is_new_mess'] = True

            msg_to_kafka = json.dumps(
                all_messages[0],
                sort_keys=True,
                indent=1,
                cls=DjangoJSONEncoder
            )

            print(msg_to_kafka)

            producer.produce(
                "faustest-5",
                msg_to_kafka,
                callback=lambda err, decoded_message, original_message=msg_to_kafka: delivery_report(  # noqa
                    err, decoded_message, original_message
                ),
            )

            producer.flush()
            latest_message_id = result[1]
            time.sleep(3)
            continue
        else:
            latest_message_id = result[1]
            time.sleep(3)
            continue





