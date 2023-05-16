from kafka import KafkaConsumer
from shuttle import Shuttle


bootstrap_servers = ["ec2-xxxx-xxx-xx.ap-northeast-2.compute.amazonaws.com:9092"]  # Kafka Broker Server
topic_name = 'test01'
shuttle = Shuttle(
    bootstrap_servers= bootstrap_servers
)
shuttle.init_consumer()
shuttle.subscribe(topic_name)
shuttle.receive()