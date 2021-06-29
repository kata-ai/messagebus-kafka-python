#!/usr/bin/env python3
"""
Defines AvroConsumer API class which exposes interface for various consumer functions
"""
import importlib
from pathlib import Path

from confluent_kafka import avro
from confluent_kafka.avro import AvroConsumer, SerializerError
from confluent_kafka.cimpl import KafkaError

from messagebus.base import Base


class Consumer(Base):
    """
    This class implements the Interface for Kafka consumer carrying Avro messages.
    It is expected that the users would extend this class and override handle_message function.
    """

    def __init__(
        self,
        conf: dict,
        value_schema_str: str,
        topics: str,
        batch_size=5,
        logger=None,
    ):
        super().__init__(logger)

        key_schema_str = ""
        with open(
            f"{Path(__file__).absolute().parent}/schemas/message_header.avsc", "r"
        ) as f:
            key_schema_str = f.read()

        reader_key_schema = avro.loads(key_schema_str)
        reader_value_schema = avro.loads(value_schema_str)

        self.consumer = AvroConsumer(
            conf,
            reader_key_schema=reader_key_schema,
            reader_value_schema=reader_value_schema,
        )
        self.running = True
        self.topics = topics
        self.batch_size = batch_size

    def shutdown(self):
        """
        Shutdown the consumer
        :return:
        """
        self.log_debug("Trigger shutdown")
        self.running = False

    @staticmethod
    def _create_instance(*, module_name: str, class_name: str):
        module = importlib.import_module(module_name)
        class_ = getattr(module, class_name)
        return class_()

    @staticmethod
    def _create_instance_with_params(*, module_name: str, class_name: str):
        module = importlib.import_module(module_name)
        class_ = getattr(module, class_name)
        return class_

    def handle_message(self, topic: str, key, value):
        """
        Process the incoming message. Must be overridden in the derived class
        :param topic: topic name
        :param key: incoming message key
        :param value: incoming message value
        :return:
        """
        self.log_debug("Message received for topic " + topic)
        self.log_debug("Key = {}".format(key))
        self.log_debug("Value = {}".format(value))

    def consume_auto(self):
        """
        Consume records unless shutdown triggered. Uses Kafka's auto commit.
        """
        self.consumer.subscribe(self.topics)

        while self.running:
            try:
                msg = self.consumer.poll(1)

                # There were no messages on the queue, continue polling
                if msg is None:
                    continue

                if msg.error():
                    self.log_error("Consumer error: {}".format(msg.error()))
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        self.log_error(
                            "%% %s [%d] reached end at offset %d\n"
                            % (msg.topic(), msg.partition(), msg.offset())
                        )
                    elif msg.error():
                        self.log_error("Consumer error: {}".format(msg.error()))
                        continue

                self.handle_message(msg.topic(), msg.key(), msg.value())
            except SerializerError as e:
                # Report malformed record, discard results, continue polling
                self.log_error("Message deserialization failed {}".format(e))
                continue
            except KeyboardInterrupt:
                break

        self.log_debug("Shutting down consumer..")
        self.consumer.close()

    def consume_sync(self):
        """
        Consume records unless shutdown triggered. Using synchronous commit after a message batch.
        """
        self.consumer.subscribe(self.topics)

        msg_count = 0
        while self.running:
            try:
                msg = self.consumer.poll(1)

                # There were no messages on the queue, continue polling
                if msg is None:
                    continue

                if msg.error():
                    self.log_error("Consumer error: {}".format(msg.error()))
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        self.log_error(
                            "%% %s [%d] reached end at offset %d\n"
                            % (msg.topic(), msg.partition(), msg.offset())
                        )
                    elif msg.error():
                        self.log_error("Consumer error: {}".format(msg.error()))
                        continue

                self.handle_message(msg.topic(), msg.key(), msg.value(), msg.headers())
                msg_count += 1
                if msg_count % self.batch_size == 0:
                    self.consumer.commit(asynchronous=False)

            except SerializerError as e:
                # Report malformed record, discard results, continue polling
                self.log_error("Message deserialization failed {}".format(e))
                continue
            except KeyboardInterrupt:
                break

        self.log_debug("Shutting down consumer..")
        self.consumer.close()
