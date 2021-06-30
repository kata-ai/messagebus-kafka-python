import unittest
import time
from pathlib import Path
import random
from kafka_message_bus.admin import AdminApi
from kafka_message_bus.consumer_v1 import Consumer
from kafka_message_bus.producer_v1 import Producer
from threading import Thread

from confluent_kafka.schema_registry import record_subject_name_strategy


class MyConsumer(Consumer):
    def __init__(
        self,
        conf: dict,
        value_schema_str: str,
        topics: str,
        batch_size: int = 5,
        logger=None,
    ):
        super().__init__(conf, value_schema_str, topics, batch_size, logger)
        self.received_message = None

    def handle_message(self, topic: str, key, value):
        self.received_message = value
        self.log_debug("Message received for topic " + topic)
        self.log_debug("Key = {}".format(key))
        self.log_debug("Value = {}".format(value))


class MyProducer(Producer):
    def __init__(self, conf, value_schema_str: str, logger=None, **kwargs):
        super().__init__(conf, value_schema_str, logger, **kwargs)
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
        pass
        # self.username = 'username'
        # self.password = 'password'
        self.schema_registry_url = "http://localhost:8081"
        self.broker = "localhost:9092"
        self.script_location = Path(__file__).absolute().parent.parent
        self.conf = {
            # default config (producer and consumer)
            # put librdkafka and/or confluent producer configuration in here
            "bootstrap.servers": self.broker,
            # 'sasl.mechanism': "SCRAM-SHA-512",
            # 'security.protocol': "SASL_PLAINTEXT",
            # 'sasl.username': username,
            # 'sasl.password': password,
        }
        # adminApi
        self.api = self._get_api()
        # create topics
        self.topic_test_1 = f"dev-python-messagebus-test{random.randint(21, 30)}"
        self.topic_test_2 = f"dev-python-messagebus-test{random.randint(31, 40)}"
        self.topics = [self.topic_test_1, self.topic_test_2]
        self.api.create_topics(self.topics)
        # create key and value schema
        self.val_schema = self._get_val_schema()
        self.producer = self._get_producer()
        self.consumer = self._get_consumer()

    def test_workflow(self):
        pass
        consume_thread = Thread(target=self.consumer.consume_auto, daemon=True)
        consume_thread.start()
        produce_result = self.producer.produce_async(
            self.topic_test_1,
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
                    # producer config
                    # put librdkafka and/or confluent consumer configuration in here
                    # and also schema registry configuration
                    "schema.registry.url": self.schema_registry_url,
                    # to custom subject name strategy put this item (only for producer)
                    "subject.name.strategy": record_subject_name_strategy,
                },
            },
            self.val_schema,
        )

    def _get_consumer(self) -> MyConsumer:
        return MyConsumer(
            {
                **self.conf,
                **{
                    # consumer config
                    # put librdkafka and/or confluent consumer configuration in here
                    # and also schema registry configuration
                    "auto.offset.reset": "earliest",
                    "group.id": "default",
                    "schema.registry.url": self.schema_registry_url,
                },
            },
            self.val_schema,
            [self.topic_test_1],
        )

    def _get_api(self) -> AdminApi:
        api = AdminApi(self.conf)
        for a in api.list_topics():
            print("Topic {}".format(a))
        return api

    def _get_val_schema(self) -> str:
        with open(f"{self.script_location}/schemas/johny_schema.avsc", "r") as f:
            val_schema = f.read()
        return val_schema
