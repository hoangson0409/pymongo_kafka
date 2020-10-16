
from confluent_kafka import Producer
import socket
import time

conf = {'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()}

producer = Producer(conf)
msg = ('kafkatest' * 20).encode()[:100]

def delivery_report(err, decoded_message, original_message):
    if err is not None:
        print(err)


for _ in range(100):
        producer.produce(
            "confluent-kp",
            msg,
            callback=lambda err, decoded_message, original_message=msg: delivery_report(  # noqa
                err, decoded_message, original_message
            ),
        )

        producer.flush()
        time.sleep(2)