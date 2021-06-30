from typing import Mapping
from uuid import uuid4
import socket
from datetime import datetime
from messagebus_kafka import __VERSION__ as version
import json

class MessageKey():

    def __init__(self, *args, **kwrags):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 1))  # connect() for UDP doesn't send packets
        ip = s.getsockname()[0]
        s.close()
        message_id = str(uuid4())

        self.value_subject = kwrags['valueSubject']  if 'valueSubject' in kwrags  else 'unknown_value_subject'
        self.message_id = message_id
        self.correlation_id = message_id
        self.conversation_id = message_id
        self.origin_service = kwrags['originService']  if 'originService' in kwrags  else 'unknown-service-name'
        self.origin_hostname = ip
        self.message_bus_version = f"python:kafka_message_bus:v{version}"
        self.timestamp = int(datetime.now().timestamp() * 1000)

    def to_dict(self) -> Mapping[str, object]:
        return {    
            'valueSubject': self.value_subject,
            'messageId': self.message_id,
            'correlationId': self.correlation_id,
            'conversationId': self.conversation_id,
            'originService': self.origin_service,
            'originHostname': self.origin_hostname,
            'messageBusVersion': self.message_bus_version,
            'timestamp': self.timestamp,
        }

    def __str__(self) -> str:
        return json.dumps(self.to_dict())
