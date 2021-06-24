import unittest
from messagebus.messages.message_header import MessageHeader

class MessageBusHeaderTest(unittest.TestCase):

	def test_new_empty_message_header_to_dict(self):
		message_header = MessageHeader()
		message_header_dict = message_header.to_dict()
		# value_subject
		self.assertEqual(message_header_dict.get('value_subject'), 'unknown_value_subject')
		# message_id
		self.assertNotEqual(message_header_dict.get('message_id'), '')
		self.assertIsNotNone(message_header_dict.get('message_id'))
		# correlation_id
		self.assertIsNotNone(message_header_dict.get('correlation_id'), message_header_dict.get('message_id'))
		# conversation_id
		self.assertIsNotNone(message_header_dict.get('conversation_id'), message_header_dict.get('message_id'))
		# origin_service
		self.assertEqual(message_header_dict.get('origin_service'), 'unknown_service_name')
		# origin_hostname
		self.assertIsNotNone(message_header_dict.get('origin_hostname'))
		self.assertNotEqual(message_header_dict.get('origin_hostname'), '')
		# message_bus_version
		self.assertIsNotNone(message_header_dict.get('message_bus_version'))
		self.assertNotEqual(message_header_dict.get('message_bus_version'), '')
		# timestamp
		self.assertIsNotNone(message_header_dict.get('timestamp'))
		self.assertNotEqual(message_header_dict.get('timestamp'), '')

