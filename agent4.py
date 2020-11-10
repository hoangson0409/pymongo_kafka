import faust
import asyncio
import datetime as dt


# Define a class inheriting faust.Record with its attributes
# content, date, is_new_message, is_trade_signal
class tlg_mess(faust.Record):
    content: str
    date: dt.datetime
    is_new_message: bool
    is_trade_signal: bool



# Declaring application
app = faust.App('agent-example-tet6')

# Declaring topic tlg_message
topic = app.topic('tlg_message', value_type=tlg_mess)

@app.agent(topic)
async def tlg_mess_printer(stream):
    async for value in stream:
        print(type(value))
        # here we receive Add objects, add a + b.
        yield value.content


