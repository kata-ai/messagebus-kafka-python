import unittest
import time
from pathlib import Path
from messagebus.admin import AdminApi
from threading import Thread

class TestConsumer(Consumer):

    def __init__(self, conf: dict, key_schema_str: str, value_schema_str: str, topics: str, batch_size: int = 5, logger=None):
        super().__init__(conf, key_schema_str, value_schema_str, topics, batch_size, logger)
        self.received_message = None


    def handle_message(self, message):
        self.received_message = message
        print('Message received: {}'.format(received_message))


class TestProducer(Producer):

    def __init__(self, conf, key_schema_str: str, value_schema_str: str, logger=None, **kwargs):
        super().__init__(conf, key_schema_str, value_schema_str, logger, **kwargs)
        self.error = None


    def delivery_report(self, err, msg, obj=None):
        if err is not None:
            self.error = err
            print('MyProducer: Error {}'.format(err))
        else:
            print('MyProducer: Successfully produced to {} [{}] at offset {}'.format(
                msg.topic(), msg.partition(), msg.offset()
            ))


class MessageBusTest(unittest.TestCase):

    def __init__(self):
        super().__init__()
        self.username = 'username'
        self.password = 'password'
        self.schema_registry_url = 'http://localhost:8081'
        self.broker='localhost:9092'
        self.script_location = Path(__file__).absolute().parent.parent
        self.conf = {
            'bootstrap.servers': self.broker,
        }
        # adminApi
        self.api = self._get_api()
        # create topics
        self.topic_test_1 = str("dev-python-messagebus-test1")
        self.topic_test_2 = str("dev-python-messagebus-test2")
        self.topics = [topic_test_1, topic_test_2]
        self.api.create_topics(self.topics)
        # create key and value schema
        self.key_schema = self._get_key_schema()
        self.val_schema = self._get_val_schema()
        self.producer = self._get_producer() 
        self.consumer = self._get_consumer()


    def test_workflow(self):
        consume_thread = Thread(target=self.consumer.consume_auto, daemon=True)
        consume_thread.start()
        produce_result = self.producer.produce_async(
            self.topic_test_1,
            {"name": "Johny", "age": 29},
        )
        # TODO: use assert instead of print
        print(produce_result)
        while self.consumer.received_message is None:
            time.sleep(1)
        self.consumer.shutdown()
        consume_thread.join()
        # TODO: assert instead of print
        print(self.consumer.received_message)


    def _get_producer(self) -> TestProducer:
        return TestProducer(
            {
                **self.conf,
                **{
                    "schema.registry.url": self.schema_registry_url,
                    # "on_delivery": on_delivery_callback,  # you can use this item to catch the produce_sync callback
                },
            },
            self.key_schema,
            self.val_schema,
        )

    
    def _get_consumer(self, topics: List[str]) -> TestConsumer:
        return TestConsumer(
            {
                **self.conf,
                **{
                    "auto.offset.reset": "earliest",
                    "group.id": "default",
                    "schema.registry.url": self.schema_registry_url,
                },
            },
            self.key_schema,
            self.val_schema,
            [self.topic_test_1],
        )


    def _get_api(self) -> AdminApi:
        api = AdminApi(conf)
        for a in api.list_topics():
            print("Topic {}".format(a))
        return api

    
    def _get_key_schema(self) -> str:
        with open(f"{self.script_location}/schemas/key.avsc", "r") as f:
            key_schema = f.read()
        return key_schema


    def _get_val_schema(self) -> str:
        with open(f"{self.script_location}/schemas/johny_schema.avsc", "r") as f:
            val_schema = f.read()
        return val_schema
