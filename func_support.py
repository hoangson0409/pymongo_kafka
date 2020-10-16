import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode




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

def consume(consumer, timeout):
    while True:
        message = consumer.poll(timeout)
        if message is None:
            continue
        if message.error():
            print("Consumer error: {}".format(message.error()))
            continue
        yield message
    consumer.close()




