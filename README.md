# Kafka Message Bus


.. image:: https://github.com/kata-ai/messagebus-kafka-python/workflows/CI/badge.svg?branch=master
    :target: https://github.com/kata-ai/messagebus-kafka-python/actions?workflow=CI
    :alt: CI Status

.. image:: https://img.shields.io/codecov/c/github/kata-ai/messagebus-kafka-python.svg?style=flat
    :alt: Codecov
    
## Overview

## Requirements

- Python 3.7+
- confluent-kafka[avro]

or install from requirements.txt

```bash
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
pytest -v
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