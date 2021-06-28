#!/usr/bin/env python3
"""
Module to test various messages
"""
import unittest
import time
from pathlib import Path


from messagebus.admin import AdminApi
from messagebus.consumer import Consumer
from messagebus.producer import Producer
from confluent_kafka.schema_registry import record_subject_name_strategy


class MessageBusTest(unittest.TestCase):
    """
    Implements test functions
    """

    username = "username"
    password = "password"
    broker = "localhost:9092"
    schema_registry_url = "http://localhost:8081"
    script_location = Path(__file__).absolute().parent.parent

    def test_consumer_producer(self):
        from threading import Thread

        conf = {
            "bootstrap.servers": self.broker,
        }
        # Create Admin API object
        api = AdminApi(conf)

        for a in api.list_topics():
            print("Topic {}".format(a))

        topic_test_1 = str("dev-python-messagebus-test1")
        topic_test_2 = str("dev-python-messagebus-test2")
        topics = [topic_test_1, topic_test_2]

        # create topics
        api.create_topics(topics)

        # load AVRO schema
        with open(f"{self.script_location}/schemas/key.avsc", "r") as f:
            key_schema = f.read()
        with open(f"{self.script_location}/schemas/johny_schema.avsc", "r") as f:
            val_schema = f.read()

        # create a producer
        class MyProducer(Producer):
            def delivery_report(self, err, msg, obj=None):
                if err is not None:
                    print("MyProducer: Error {}".format(err))
                else:
                    print(
                        "MyProducer: Successfully produced to {} [{}] at offset {}".format(
                            msg.topic(), msg.partition(), msg.offset()
                        )
                    )

        producer = MyProducer(
            {
                **conf,
                **{
                    "schema.registry.url": self.schema_registry_url,
                    'subject.name.strategy': record_subject_name_strategy,
                    # "on_delivery": on_delivery_callback,  # you can use this item to catch the produce_sync callback
                },
            },
            key_schema,
            val_schema,
        )

        # push messages to topics
        result = producer.produce_async(
            topic_test_1,
            {"name": "Johny", "age": 29},
        )
        print(result)

        # Fallback to earliest to ensure all messages are consumed
        print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        print("++++++++++++++++++++++CONSUMER+++++++++++++++++++++")
        print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

        class TestConsumer(Consumer):
            def set_parent(self, parent):
                self.parent = parent

            def handle_message(self, message):
                print("message:", message)

        # create a consumer
        consumer = TestConsumer(
            {
                **conf,
                **{
                    "auto.offset.reset": "earliest",
                    "group.id": "default",
                    "schema.registry.url": self.schema_registry_url,
                },
            },
            key_schema,
            val_schema,
            [topic_test_1],
        )
        consumer.set_parent(self)

        # start a thread to consume messages
        consume_thread = Thread(target=consumer.consume_auto, daemon=True)

        consume_thread.start()
        time.sleep(10)

        # trigger shutdown
        consumer.shutdown()

        # delete topics
        api.delete_topics(topics)

    def validate_message(self, incoming, outgoing):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.age, outgoing.age)


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 5:
        sys.stderr.write(
            "Usage: python %s <bootstrap-brokers> <schema-registry-url> <username> <password>\n"
            % sys.argv[0]
        )
        sys.exit(1)

    MessageBusTest.password = sys.argv.pop()
    MessageBusTest.username = sys.argv.pop()
    MessageBusTest.schema_registry_url = sys.argv.pop()
    MessageBusTest.broker = sys.argv.pop()
    MessageBusTest.script_location = Path(__file__).absolute().parent.parent

    unittest.main()
