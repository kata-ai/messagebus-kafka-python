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

The example is available in this [test](./messagebus/test/message_workflow_v1_test.py)

#### **Producer and Consumers V2**

The example is available in this [test](./messagebus/test/message_workflow_v2_test.py)

### **Testing**

```bash
cd messagebus
pytest -v -rPx
```