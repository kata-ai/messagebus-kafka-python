# **Kafka Message Bus**

![image](https://github.com/kata-ai/messagebus-kafka-python/workflows/CI/badge.svg?branch=master%0A%20:target:%20https://github.com/kata-ai/messagebus-kafka-python/actions?workflow=CI%0A%20:alt:%20CI%20Status)

[![codecov](https://codecov.io/gh/kata-ai/messagebus-kafka-python/branch/master/graph/badge.svg?token=SV5XR0IFM5)](https://codecov.io/gh/kata-ai/messagebus-kafka-python)
## **Overview**

## **Requirements**

- Python 3.6+
- confluent-kafka[avro]


## **Dev Requirements**

```bash
docker-compose up -d

pip install pipenv
pipenv install
pipenv shell
```

## **Documentaion**

- Configuration properties in [librdkafka](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
- Confluent producer configuration in [here](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html)
- Confluent consumer configuration in [here](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html)
- Python confluent kafka client in [here](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)


## **Installation**

```bash
pip3 install .
```

## **Usage**

This package implements the interface for producer/consumer APIs to push/read messages to/from Kafka via AvroSerializer.

### **Examples**

#### **Producers and Consumer V1**

The example is available in this [test](messagebus_kafka/test/message_workflow_v1_test.py)

- **Producer** implementation
```python
 class MyProducer(Producer):

    # kafka delivery callback handler
    def delivery_report(self, err, msg, obj=None):
        if err is not None:
            # error handler
            # code here
            pass 
        else:
            # success handler
            # code here
            pass

```
- **Consumer** implementation
```python
class MyConsumer(Consumer):

    # message handler overrider
    def handle_message(self, topic: str, key, value):
        # code here
        pass
```

- Produce a message
```python
producer = MyProducer(
    {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081"
    },
    "<string(json string) avro schema of value>",
)
produce_result = producer.produce_async(
    "test_topic",
    {"name": "Johny", "age": 29},
)
```
- Consume a message
```python
consumer = MyConsumer(
    {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081"
        "auto.offset.reset": "earliest",
        "group.id": "default",
    },
    "<string(json string) avro schema of value>",
    "test_topic",
)
consume_thread = Thread(target=consumer.consume_auto, daemon=True)
consume_thread.start()
consumer.shutdown()
consume_thread.join()
``` 

#### **Producer and Consumers V2**

The example is available in this [test](messagebus_kafka/test/message_workflow_v2_test.py)


- **Producer** implementation
```python
 class MyProducer(Producer):

    # kafka delivery callback handler
    def delivery_report(self, err, msg, obj=None):
        if err is not None:
            # error handler
            # code here
            pass 
        else:
            # success handler
            # code here
            pass

```
- **Consumer** implementation
```python
class MyConsumer(Consumer):

    # message handler overrider
    def handle_message(self, topic: str, key, value, headers: dict):
        # code here
        pass
```

- Produce a message
```python
producer = MyProducer(
    {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081"
    },
    "<string(json string) avro schema of value>",
    "<string(json string) avro schema of key>",
)
produce_result = producer.produce_async(
    "test_topic",
    {"name": "Johny", "age": 29},
    key="<UUID>" # optional (when use custom key schema)
)
```
- Consume a message
```python
consumer = MyConsumer(
    {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081"
        "auto.offset.reset": "earliest",
        "group.id": "default",
    },
    "<string(json string) avro schema of value>",
    "test_topic",
    "<string(json string) avro schema of key>",
)
consume_thread = Thread(target=consumer.consume_auto, daemon=True)
consume_thread.start()
consumer.shutdown()
consume_thread.join()
```
### **Testing**

```bash
tox -e py

# or 

cd messagebus
pytest -v -rPx
```
