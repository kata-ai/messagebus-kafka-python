#!/usr/bin/env python3
"""
Defines Producer class which exposes interface for various producer functions
"""
import traceback
from pathlib import Path

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
    topic_subject_name_strategy,
)
from messagebus.base import Base
from messagebus.messages.message_header import MessageHeader
from confluent_kafka.schema_registry.avro import AvroSerializer


class Producer(Base):
    """
    This class implements the Interface for Kafka producer carrying Avro messages.
    It is expected that the users would extend this class and override on_delivery function.
    """

    use_default_key_schema = False

    def __init__(
        self, conf, value_schema_str: str, key_schema_str=None, logger=None, **kwargs
    ):
        """
        Initialize the Producer
        :param conf: configuration e.g:
                    {'bootstrap.servers': localhost:9092,
                    'schema.registry.url': http://localhost:8083}
        :param key_schema: loaded avro schema_str for the key
        :param value_schema: loaded avro schema_str for the value
        """
        super().__init__(logger)

        if not key_schema_str:
            self.use_default_key_schema = True

            with open(f"{Path(__file__).absolute().parent}/schemas/key.avsc", "r") as f:
                key_schema_str = f.read()

        if not ("subject.name.strategy" in conf):
            conf["subject.name.strategy"] = topic_subject_name_strategy

        schema_registry_client = SchemaRegistryClient(
            {
                "url": conf["schema.registry.url"],
            }
        )

        key_serializer = AvroSerializer(
            schema_str=key_schema_str,
            schema_registry_client=schema_registry_client,
            conf={
                "auto.register.schemas": True,
                "subject.name.strategy": conf["subject.name.strategy"],
            },
        )

        value_serializer = AvroSerializer(
            schema_str=value_schema_str,
            schema_registry_client=schema_registry_client,
            conf={
                "auto.register.schemas": True,
                "subject.name.strategy": conf["subject.name.strategy"],
            },
        )

        serializer_conf = {
            "key.serializer": key_serializer,
            "value.serializer": value_serializer,
        }

        del conf["schema.registry.url"]
        del conf["subject.name.strategy"]

        conf.update(serializer_conf)
        self.producer = SerializingProducer(conf)

    def set_logger(self, logger):
        """
        Set logger
        :param logger: logger
        """
        self.logger = logger

    def delivery_report(self, err, msg, obj=None):
        """
        Handle delivery reports served from producer.poll.
        This callback takes an extra argument, obj.
        This allows the original contents to be included for debugging purposes.
        """
        if err is not None:
            self.log_error("Error {}".format(err))
        else:
            self.log_debug(
                "Successfully produced to {} [{}] at offset {}".format(
                    msg.topic(), msg.partition(), msg.offset()
                )
            )

    def __message_header_generator(self) -> dict:
        message_header = MessageHeader()
        return message_header.to_dict()

    def produce_async(self, topic: str, value, key=None) -> bool:
        """
        Produce records for a specific topic
        :param topic: topic to which messages are written to
        :param record: record/message to be written
        :param on_delivery_callback (callable(KafkaError, Message), optional): Delivery
            report callback to call (from
            :py:func:`SerializingProducer.poll` or
            :py:func:`SerializingProducer.flush` on successful or
            failed delivery.
        :return:
        """
        self.log_debug("Producing records to topic {}.".format(topic))
        try:
            # The message passed to the delivery callback will already be serialized.
            # To aid in debugging we provide the original object to the delivery callback.
            if not self.use_default_key_schema and not key:
                raise KeyError("Key cannot be empty.")

            headers = self.__message_header_generator()
            if self.use_default_key_schema:
                key = headers["message_id"]

            self.producer.produce(
                topic=topic,
                key=key,
                value=value,
                headers=headers,
                on_delivery=self.delivery_report,
            )
            # Serve on_delivery callbacks from previous asynchronous produce()
            self.producer.poll(0)
            return True
        except ValueError as ex:
            traceback.print_exc()
            self.log_error("Invalid input, discarding record...{}".format(ex))
        return False

    def produce_sync(self, topic: str, value, key=None) -> bool:
        """
        Produce records for a specific topic
        :param topic: topic to which messages are written to
        :param record: record/message to be written
        :return:
        """
        try:
            if not self.use_default_key_schema and not key:
                raise KeyError("Key cannot be empty.")

            self.log_debug("Record type={}".format(type(value)))
            self.log_debug("Producing key {} to topic {}.".format(key, topic))
            self.log_debug("Producing record {} to topic {}.".format(value, topic))

            # Pass the message synchronously
            headers = self.__message_header_generator()
            if self.use_default_key_schema:
                key = headers["message_id"]

            self.producer.produce(
                topic=topic,
                key=key,
                value=value,
                headers=headers,
            )
            self.producer.flush()
            return True
        except ValueError as ex:
            self.log_error("Invalid input, discarding record...")
            self.log_error(f"Exception occurred {ex}")
            self.log_error(traceback.format_exc())
        except Exception as ex:
            self.log_error(f"Exception occurred {ex}")
            self.log_error(traceback.format_exc())
        return False
