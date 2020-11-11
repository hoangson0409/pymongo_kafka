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

table = app.Table(
    'tlg_message_table0', default=int, partitions=1)

table2 = app.Table(
    'tlg_message_tradesignal_table', default=int, partitions=1)

@app.agent()
async def tlg_mess_printer_new_tradesignal(stream):
    async for value in stream:
        #.filter(lambda value : value.is_new_message == True) :
        print('****************************************')
        print('New Trade Signal Agent Info')
        print('here is the value: ', value, ' with type ', type(value))

        key = value.date.split('T')[0]
        print(key,type(key))
        if value.is_trade_signal:
            table2[key] += 1

        print(table2[key])
        print('#########################################')

        # here we receive Add objects, add a + b.
        yield value.content

@app.agent(topic,sink=[tlg_mess_printer_new_tradesignal])
async def tlg_mess_printer_new_message(stream):
    async for value in stream:
        #.filter(lambda value : value.is_new_message == True) :
        print('****************************************')
        print('New Message Agent Info')
        print('here is the value: ',value, ' with type ',type(value))
        key = value.date.split('T')[0]
        print(key,type(key))
        if value.is_new_message:
            table[key] += 1

        print(table[key])
        print('#########################################')

        # here we receive Add objects, add a + b.
        yield value


# @app.agent(topic,sink=[tlg_mess_printer2])
# async def tlg_mess_printer(stream):
#     async for value in stream :
#         print('FIRST AGENT SPEAKING')
#
#         yield value
if __name__ == '__main__':
    app.main()