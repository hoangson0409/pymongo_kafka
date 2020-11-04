import faust
import asyncio
import json


# The model describes the data sent to our agent,
# We will use a JSON serialized dictionary
# with two integer fields: a, and b.
class Add(faust.Record):
    a: int
    b: int

# Next, we create the Faust application object that
# configures our environment.
app = faust.App(
    'faust-app',
    broker='kafka://localhost:9092',
    value_serializer='raw',
)

# The Kafka topic used by our agent is named 'adding',
# and we specify that the values in this topic are of the Add model.
# (you can also specify the key_type if your topic uses keys).
topic = app.topic('faustest-1')

@app.agent(topic)
async def printer(stream):
    async for value in stream:
        print(type(value))
        print(value)
        print(type(value.decode('utf-8')))
        print(value.decode('utf-8'))
        print(json.loads(value.decode('utf-8')))
        print(type(json.loads(value.decode('utf-8'))))

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