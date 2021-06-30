from typing import Mapping
from uuid import uuid4
import socket
from datetime import datetime
from messagebus_kafka import __VERSION__ as version
import json

class MessageHeader():

    def __init__(self, *args, **kwrags):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 1))  # connect() for UDP doesn't send packets
        ip = s.getsockname()[0]
        s.close()
        message_id = str(uuid4())

        self.value_subject = kwrags['value_subject']  if 'value_subject' in kwrags  else 'unknown_value_subject'
        self.message_id = message_id
        self.correlation_id = message_id
        self.conversation_id = message_id
        self.origin_service = kwrags['origin_service']  if 'origin_service' in kwrags  else 'unknown-service-name'
        self.origin_hostname = ip
        self.message_bus_version = f"python:kafka_message_bus:v{version}"
        self.timestamp = str(int(datetime.now().timestamp() * 1000))

    def to_dict(self) -> Mapping[str, str]:
        return {
            'value_subject': self.value_subject,
            'message_id': self.message_id,
            'correlation_id': self.correlation_id,
            'conversation_id': self.conversation_id,
            'origin_service': self.origin_service,
            'origin_hostname': self.origin_hostname,
            'message_bus_version': self.message_bus_version,
            'timestamp': self.timestamp,
        }

    def __str__(self) -> str:
        return json.dumps(self.to_dict())
