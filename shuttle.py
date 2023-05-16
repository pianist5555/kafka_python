from time import sleep
from json import dumps
from kafka import KafkaProducer, KafkaConsumer


class Shuttle(object):
    def __init__(
        self,
        bootstrap_servers: list = []
    ):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.consumer = None

    def init_producer(
        self,
    ):
        self.producer = KafkaProducer(
            acks=0,
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: dumps(x).encode('utf-8'),
        )

    def init_consumer(
        self,
    ):
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=100,
        )

    def subscribe(
            self,
            topic: str = "failure",
        ):
        self.consumer.subscribe(topics=[topic])

    def send(
        self,
        topic: str = "failure",
        value: any = None,
    ):
        self.producer.send(topic, value)
        self.producer.flush()

    def receive(
        self,
    ):
        while True:
            sleep(1)
            for message in self.consumer:
                print('* 토픽명:', message.topic, "메세지: ", message.value, "오프셋: ", message.offset)