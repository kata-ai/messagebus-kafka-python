import unittest
import time
from pathlib import Path
from messagebus.admin import AdminApi
from messagebus.consumer import Consumer
from messagebus.producer import Producer
from messagebus import __VERSION__ as version
from threading import Thread

from confluent_kafka.schema_registry import record_subject_name_strategy    
from datetime import datetime
from uuid import uuid4

class MyConsumer(Consumer):

    def __init__(self, conf: dict, key_schema_str: str, value_schema_str: str, topics: str, batch_size: int = 5, logger=None):
        super().__init__(conf, key_schema_str, value_schema_str, topics, batch_size, logger)
        self.received_message = None


    def handle_message(self, topic: str, key: dict, value: dict, headers: dict):
        self.received_message = value
        self.log_debug("Message received for topic " + topic)
        self.log_debug("Key = {}".format(key))
        self.log_debug("Value = {}".format(value))
        self.log_debug("Headers = {}".format(headers))

class MyProducer(Producer):

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

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        # self.username = 'username'
        # self.password = 'password'
        self.schema_registry_url = 'http://localhost:8081'
        self.broker='localhost:9092'
        self.script_location = Path(__file__).absolute().parent.parent
        self.conf = {
            # default config (producer and consumer)
            # put librdkafka and/or confluent producer configuration in here
            'bootstrap.servers': self.broker,
            # 'sasl.mechanism': "SCRAM-SHA-512",
            # 'security.protocol': "SASL_PLAINTEXT",
            # 'sasl.username': username,
            # 'sasl.password': password,
        }
        # adminApi
        self.api = self._get_api()
        # create topics
        self.topic_test_1 = str("dev-python-messagebus-test1")
        self.topic_test_2 = str("dev-python-messagebus-test2")
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
        key = self._create_custom_key()
        produce_result = self.producer.produce_async(
            self.topic_test_1,
            {'name': 'Johny', 'age': 29},
            key=key,
        )
        print("producer's produce_async result", produce_result)
        self.assertTrue(produce_result)
        while self.consumer.received_message is None:
            time.sleep(1)

        self.consumer.shutdown()
        consume_thread.join()
        print("consumer's received_message", self.consumer.received_message)
        self.assertEqual(self.consumer.received_message['name'], 'Johny')
        self.assertEqual(self.consumer.received_message['age'], 29)

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
                    'subject.name.strategy': record_subject_name_strategy,
                },
            },
            self.key_schema,
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
            self.key_schema,
            self.val_schema,
            [self.topic_test_1],
        )


    def _get_api(self) -> AdminApi:
        api = AdminApi(self.conf)
        for a in api.list_topics():
            print("Topic {}".format(a))
        return api

    
    def _get_key_schema(self) -> str:
        with open(f"{self.script_location}/schemas/message_header.avsc", "r") as f:
            key_schema = f.read()
        return key_schema


    def _get_val_schema(self) -> str:
        with open(f"{self.script_location}/schemas/johny_schema.avsc", "r") as f:
            val_schema = f.read()
        return val_schema

    def _create_custom_key(self) -> dict:
        message_id = str(uuid4())
        return {
            "valueSubject": "test_value_subject",
            "messageId": message_id,
            "correlationId": message_id,
            "conversationId": message_id,
            "originService": "unknown_origin_service",
            "replyTopic": None,
            "originHostname": "127.0.0.1",
            "messageBusVersion": f"python:kafka_message_bus:v{version}",
            "timestamp": datetime.now().timestamp() * 1000,
        }