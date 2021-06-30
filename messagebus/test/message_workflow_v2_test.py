import unittest
import time
from pathlib import Path
import random
from messagebus.admin import AdminApi
from messagebus.consumer_v2 import Consumer
from messagebus.producer_v2 import Producer

from threading import Thread


class MyConsumer(Consumer):
    def __init__(
        self,
        conf: dict,
        value_schema_str: str,
        topics: str,
        key_schema_str: str = None,
        batch_size: int = 5,
        logger=None,
    ):
        super().__init__(
            conf, value_schema_str, topics, key_schema_str, batch_size, logger
        )
        self.received_message = None

    def handle_message(self, topic: str, key, value, headers: dict):
        self.received_message = value
        self.log_debug("Message received for topic " + topic)
        self.log_debug("Key = {}".format(key))
        self.log_debug("Value = {}".format(value))
        self.log_debug("Headers = {}".format(headers))


class MyProducer(Producer):
    def __init__(
        self,
        conf,
        value_schema_str: str,
        key_schema_str: str = None,
        logger=None,
        **kwargs,
    ):
        super().__init__(conf, value_schema_str, key_schema_str, logger, **kwargs)
        self.error = None

    def delivery_report(self, err, msg, obj=None):
        if err is not None:
            self.error = err
            print("MyProducer: Error {}".format(err))
        else:
            print(
                "MyProducer: Successfully produced to {} [{}] at offset {}".format(
                    msg.topic(), msg.partition(), msg.offset()
                )
            )


class MessageBusTest(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super().__init__(methodName)
        self.username = "username"
        self.password = "password"
        self.schema_registry_url = "http://localhost:8081"
        self.broker = "localhost:9092"
        self.script_location = Path(__file__).absolute().parent.parent
        self.conf = {
            "bootstrap.servers": self.broker,
        }
        # adminApi
        self.api = self._get_api()
        # create topics
        self.topic_test_1 = f"dev-python-messagebus-test{random.randint(1, 10)}"
        self.topic_test_2 = f"dev-python-messagebus-test{random.randint(11, 20)}"
        self.topics = [self.topic_test_1, self.topic_test_2]
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
            self.topic_test_2,
            {"name": "Johny", "age": 29},
        )
        print("producer's produce_async result", produce_result)
        self.assertTrue(produce_result)
        while self.consumer.received_message is None:
            time.sleep(1)
        self.consumer.shutdown()
        consume_thread.join()
        print("consumer's received_message", self.consumer.received_message)
        self.assertEqual(self.consumer.received_message["name"], "Johny")
        self.assertEqual(self.consumer.received_message["age"], 29)

        # delete topics
        self.api.delete_topics(self.topics)

    def _get_producer(self) -> MyProducer:
        return MyProducer(
            {
                **self.conf,
                **{
                    "schema.registry.url": self.schema_registry_url,
                    # "on_delivery": on_delivery_callback,  # you can use this item to catch the produce_sync callback
                },
            },
            self.val_schema,
            self.key_schema,
        )

    def _get_consumer(self) -> MyConsumer:
        return MyConsumer(
            {
                **self.conf,
                **{
                    "auto.offset.reset": "earliest",
                    "group.id": "default",
                    "schema.registry.url": self.schema_registry_url,
                },
            },
            self.val_schema,
            [self.topic_test_2],
            self.key_schema,
        )

    def _get_api(self) -> AdminApi:
        api = AdminApi(self.conf)
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
