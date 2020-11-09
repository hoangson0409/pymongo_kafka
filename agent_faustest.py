import faust
import asyncio
import json
import datetime as dt

class Tlg_message(faust.Record):
    content: str
    date: dt.datetime
    is_new_message: bool
    is_trade_signal: bool


app = faust.App(
    'faust-apple',
    broker='kafka://localhost:9092',
    value_serializer='raw',
    topic_partitions=1,
)

topic = app.topic('faustest-6',partitions=1,value_type=Tlg_message)

# table = app.Table(
#     'test1', default=int, partitions=1)



@app.agent(topic)
async def printer(stream):
    async for value in stream:
        print(value)
        print(type(value))




















# value = json.loads(value.decode('utf-8'))
        # print('here is date value of message: ', value['date'] )
        # print('here is date type value of message: ', type(value['date']))
        # table[value['date']] += 1
        # print(table[value['date']])
        # print("here is all value variable: ", value)
        # print(type(value))
        # print(value['is_new_mess'])
        # print(type(value['is_new_mess']))
        #
        # print('here is the value from date field',value['date'])
        # print(type(value['date']))





