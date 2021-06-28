import unittest
from pathlib import Path


class TestConsumer(Consumer):

	def __init__(self, conf: dict, key_schema_str: str, value_schema_str: str, topics: str, batch_size: int = 5, logger=None):
		super().__init__(conf, key_schema_str, value_schema_str, topics, batch_size, logger)
		self.received_message = None

	def handle_message(self, message):
		self.received_message = message
		print('Message received: {}'.format(received_message))


class TestProducer(Producer):

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

	def __init__(self):
		super().__init__()
		self.username = 'username'
		self.password = 'password'
		self.schema_registry_url = 'http://localhost:8081'
		self.script_location = Path(__file__).absolute().parent.parent
		self.conf = {
            'bootstrap.servers': self.broker,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.username': self.username,
            'sasl.password': self.password,
        }
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




	def test_workflow(self):
		pass
