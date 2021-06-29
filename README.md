# Kafka Message Bus

![image](https://github.com/kata-ai/messagebus-kafka-python/workflows/CI/badge.svg?branch=master%0A%20:target:%20https://github.com/kata-ai/messagebus-kafka-python/actions?workflow=CI%0A%20:alt:%20CI%20Status)

![image](https://img.shields.io/codecov/c/github/kata-ai/messagebus-kafka-python.svg%0A%20:target:%20http://codecov.io/github/kata-ai/messagebus-kafka-python?branch=master%0A%20:alt:%20Coverage%20report)

## Overview

## Requirements

- Python 3.7+
- confluent-kafka[avro]

or install from requirements.txt

```bash
docker-compose up
pip install -r requirements.txt 
```

## Installation

```bash
pip3 install .
```

## Usage

This package implements the interface for producer/consumer APIs to push/read messages to/from Kafka via AvroSerializer.

## Testing

```bash
cd messagebus
pytest -v -rPx
```

### Examples
#### Producers

Example for usage available at the end of [here](./messagebus/producer.py)

Run the example with this command:

```bash
python producer.py "<bootstrap-brokers>" "<schema-registry-url>" "<username>" "<password>"
```

#### Consumers

Example for usage available at [here](./messagebus/test/messagebus_test.py)

Run the example with this command:

```bash
python messagebus_test.py "<bootstrap-brokers>" "<schema-registry-url>" "<username>" "<password>"
```