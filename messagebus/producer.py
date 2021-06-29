#!/usr/bin/env python3
"""
Defines Producer class which exposes interface for various producer functions
"""
import traceback

from confluent_kafka import avro, SerializingProducer
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
)
from messagebus.base import Base
from messagebus.messages.message_header import MessageHeader
from confluent_kafka.schema_registry.avro import AvroSerializer
from messagebus import __VERSION__ as version


class Producer(Base):
    """
    This class implements the Interface for Kafka producer carrying Avro messages.
    It is expected that the users would extend this class and override on_delivery function.
    """

    def __init__(
        self, conf, key_schema_str: str, value_schema_str: str, logger=None, **kwargs
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
        
        default_key_schema = avro.loads(key_schema_str)
        default_value_schema = avro.loads(value_schema_str)

        if 'subject.name.strategy' in conf:
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
                    "subject.name.strategy": conf['subject.name.strategy'],
                },
            )

            value_serializer = AvroSerializer(
                schema_str=value_schema_str,
                schema_registry_client=schema_registry_client,
                conf={
                    "auto.register.schemas": True,
                    "subject.name.strategy": conf['subject.name.strategy'],
                },
            )

            serializer_conf = {
                "key.serializer": key_serializer,
                "value.serializer": value_serializer,
            }

            del conf["schema.registry.url"]
            del conf['subject.name.strategy']

            conf.update(serializer_conf)
            self.producer = SerializingProducer(conf)
        else:
            self.producer = AvroProducer(
                conf,
                default_key_schema=default_key_schema,
                default_value_schema=default_value_schema,
            )

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
            self.log_error('Error {}'.format(err))
        else:
            self.log_debug('Successfully produced to {} [{}] at offset {}'.format(
                msg.topic(), msg.partition(), msg.offset()))

    def __message_header_generator(self) -> dict:
        message_header = MessageHeader()
        return message_header.to_dict()

    def produce_async(
        self, topic: str, value, key="default"
    ) -> bool:
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
            self.producer.produce(
                topic=topic,
                key=key,
                value=value,
                headers=self.__message_header_generator(),
                on_delivery=self.delivery_report,
            )
            # Serve on_delivery callbacks from previous asynchronous produce()
            self.producer.poll(0)
            return True
        except ValueError as ex:
            traceback.print_exc()
            self.log_error("Invalid input, discarding record...{}".format(ex))
        return False

    def produce_sync(self, topic: str, value, key="default") -> bool:
        """
        Produce records for a specific topic
        :param topic: topic to which messages are written to
        :param record: record/message to be written
        :return:
        """
        try:
            self.log_debug("Record type={}".format(type(value)))
            self.log_debug("Producing key {} to topic {}.".format(key, topic))
            self.log_debug("Producing record {} to topic {}.".format(value, topic))

            # Pass the message synchronously
            self.producer.produce(
                topic=topic,
                key=key,
                value=value,
                headers=self.__message_header_generator(),
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


if __name__ == "__main__":
    import sys
    import time
    from messagebus.admin import AdminApi
    from confluent_kafka.schema_registry import record_subject_name_strategy

    if len(sys.argv) != 5:
        sys.stderr.write(
            "Usage: python %s <bootstrap-brokers> <schema-registry-url> <username> <password>\n"
            % sys.argv[0]
        )
        sys.exit(1)

    broker = sys.argv[1]
    schema_registry_url = sys.argv[2]
    username = sys.argv[3]
    password = sys.argv[4]

    # conf init
    test_conf = {
        "bootstrap.servers": broker,
        "sasl.mechanism": "SCRAM-SHA-512",
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.username": username,
        "sasl.password": password,
    }

    # create Admin API object
    api = AdminApi(test_conf)

    topics = ["dev-topic1", "dev-topic2"]

    # create topics
    api.create_topics(topics)

    # on_delivery callback function
    def on_delivery_callback(err, msg, obj=None):
        if err is not None:
            print("error {}".format(err))
        else:
            print(
                "successfully produced to {} [{}] at offset {}".format(
                    msg.topic(), msg.partition(), msg.offset()
                )
            )

    # Serializer conf
    test_conf.update(
        {
            "schema.registry.url": schema_registry_url,
            # 'schema.registry.subject.name.strategy': record_subject_name_strategy,
            # 'message.max.bytes': 1000,
            # "on_delivery": on_delivery_callback,  # you can use this item to catch the produce_sync callback
        }
    )

    # without key <----------------------
    # load AVRO schema
    test_key_schema = open("schemas/key.avsc", "r").read()
    test_val_schema = open("schemas/johny_schema.avsc", "r").read()

    # create a producer
    producer = Producer(test_conf, test_key_schema, test_val_schema)

    try:
        result = producer.produce_async(
            "dev-topic1",
            {"name": "Johny", "age": 29},
        )
        print(result)

        result = producer.produce_sync("dev-topic1", {"name": "Johny", "age": 29})
        print(result)

    except Exception as e:
        print("error: ", e)

    # with key <----------------------
    # load AVRO schema
    test_key_schema = open("schemas/message_header.avsc", "r").read()
    test_val_schema = open("schemas/johny_schema.avsc", "r").read()

    # create a producer
    test_conf.update({
        'subject.name.strategy': record_subject_name_strategy,
    })
    producer = Producer(test_conf, test_key_schema, test_val_schema)

    from datetime import datetime
    from uuid import uuid4

    message_id = str(uuid4())
    key = {
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
    try:
        result = producer.produce_async(
            "dev-topic2",
            {"name": "Johny", "age": 29},
            key=key,
        )
        print(result)

        result = producer.produce_sync(
            "dev-topic2", {"name": "Johny", "age": 29}, key=key
        )
        print(result)

    except Exception as e:
        print("error: ", e)



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
    
    test_conf.update({
        "schema.registry.url": schema_registry_url,
        'subject.name.strategy': record_subject_name_strategy,
    })
    producer = MyProducer(test_conf, test_key_schema, test_val_schema)
    message_id = str(uuid4())
    key = {
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
    try:
        result = producer.produce_async(
            "dev-topic2",
            {"name": "Johny", "age": 29},
            key=key,
        )
        print(result)

        result = producer.produce_sync(
            "dev-topic2", {"name": "Johny", "age": 29}, key=key
        )
        print(result)

    except Exception as e:
        print("error: ", e)

    time.sleep(60)
    # delete topics
    api.delete_topics(topics)
