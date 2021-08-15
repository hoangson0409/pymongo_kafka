# pymongo_kafka
This project demonstrate Telegram message reader using Kafka producer,  then using NoSQL DB to store all message. This project should later be integrated into Tele2Meta apps

2 main branchs in this repository: 
- faust-stream demonstrate a solution with confluent-kafka as main Kafka Producer and faust as consumer 
- faust-stream-full demonstrate a solution with faust API as an end to end producer and consumer

### To use Faust Stream API

- Run zookeeper and 3 Kafka servers
- `faust -A agent4 worker -l info`
- `python main_producer_faust.py`

