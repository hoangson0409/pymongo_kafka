import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
from agent_faustest import Tlg_message
import datetime as dt



def is_new_message(all_messages,latest_message_id):
    if  (
        "message" in all_messages[0].keys() and  #Must be a message
        all_messages[0]['id'] != latest_message_id #Must have different ID from the last message
        ):
        return True
    else:
        return False


def db_insert(latest_mess_id, content):
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='telegram',
                                             user='root',
                                             password='password')

        cursor = connection.cursor()

        mySql_insert_query = """INSERT INTO telegram_messages (mess_id,content) VALUES ({v1},'{v2}') """.format(v1=latest_mess_id,
                                                                                                                 v2=content)
        cursor.execute(mySql_insert_query)
        connection.commit()
        print("Records inserted successfully into table")
        cursor.close()

    except Error as error:
        print("Failed to insert record into table. Error {}".format(error))

    finally:
        if (connection.is_connected()):
            connection.close()
            print("MySQL connection is closed")

def delivery_report(err, decoded_message, original_message):
    if err is not None:
        print(err)


def hasNumbers(inputString):
    return any(char.isdigit() for char in inputString)

def is_tradesignal(all_messages):
    if  (
        "message" in all_messages[0].keys() and  #Must be a message
        ('ENTRY' in all_messages[0]['message'] or 'Entry' in all_messages[0]['message']) and #MUST HAVE ENTRY or Entry
        ('BUY' in all_messages[0]['message'] or 'SELL' in all_messages[0]['message'] or
        'Buy' in all_messages[0]['message'] or 'Sell' in all_messages[0]['message'] or
        'buy' in all_messages[0]['message'] or 'sell' in all_messages[0]['message'] ) and  #Must contain the word buy or sell
        hasNumbers(all_messages[0]['message'])  #Must have number within
        ):
        return True
    else:
        return False



