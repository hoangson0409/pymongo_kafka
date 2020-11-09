import faust
import asyncio
import json

app = faust.App(
    'faust-apple',
    broker='kafka://localhost:9092',
    value_serializer='raw',
    topic_partitions=1,
)

topic = app.topic('faustest-5',partitions=1)

table = app.Table(
    'test1', default=int, partitions=1)

@app.agent(topic)
async def printer(stream):
    async for value in stream:
        value = json.loads(value.decode('utf-8'))

        print('here is date value of message: ', value['date'] )
        print('here is date type value of message: ', type(value['date']))
        table[value['date']] += 1
        print(table[value['date']])
        # print("here is all value variable: ", value)
        # print(type(value))
        # print(value['is_new_mess'])
        # print(type(value['is_new_mess']))
        #
        # print('here is the value from date field',value['date'])
        # print(type(value['date']))





# @app.agent(topic)
# async def printer(stream):
#     async for value in stream:
#         print(type(value))
#         print(value)
#
# @app.agent(topic)
# async def adding(stream):
#     async for value in stream:
#         print(type(stream))
#         # here we receive Add objects, add a + b.
#         yield value.a + value.b
#
# async def send_value() -> None:
#     print(await adding.ask(Add(a=4, b=4)))
#
# if __name__ == '__main__':
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(send_value())