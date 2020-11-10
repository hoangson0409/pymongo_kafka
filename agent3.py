import faust
import asyncio
import datetime as dt


# The model describes the data sent to our agent,
# We will use a JSON serialized dictionary
# with two integer fields: a, and b.
class random1(faust.Record):
    a: int
    b: int
    c: bool
    d: dt.datetime



# Next, we create the Faust application object that
# configures our environment.
app = faust.App('agent-example-tet5')

# The Kafka topic used by our agent is named 'adding',
# and we specify that the values in this topic are of the Add model.
# (you can also specify the key_type if your topic uses keys).
topic = app.topic('minuzz', value_type=random1)

@app.agent(topic)
async def minuzz(stream):
    async for value in stream:
        print(type(value))
        # here we receive Add objects, add a + b.
        yield value.a * value.b


# async def send_value() -> None:
#     print(await adding.ask(Add(a=4, b=4)))
#
#
# if __name__ == '__main__':
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(send_value())