"""
Defines Admin API class which exposes interface for various admin client functions
"""
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions
from kafka_message_bus.base import Base


class AdminApi(Base):
    """
    Implements interface for Admin APIs
    """

    def __init__(self, conf, logger=None):
        super().__init__(logger)
        self.admin_client = AdminClient(conf)

    def create_topics(
        self, topics, num_partitions: int = 3, replication_factor: int = 1
    ):
        """
        Create a list of topics
        :param topics: list of topics to be created
        :param num_partitions: number of partitions for the topic
        :param replication_factor: replication factor
        :return:
        """

        new_topics = [
            NewTopic(
                topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
            for topic in topics
        ]

        # Call create_topics to asynchronously create topics, a dict
        # of <topic,future> is returned.
        fs = self.admin_client.create_topics(new_topics)

        # Wait for operation to finish.
        # Timeouts are preferably controlled by passing request_timeout=15.0
        # to the create_topics() call.
        # All futures will finish at the same time.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                self.log_debug("Topic {} created".format(topic))
            except Exception as e:
                self.log_error("Failed to create topic {}: {}".format(topic, e))

    def delete_topics(self, topics):
        """
        delete list of topics
        :param topics: list of topics
        :return:
        """

        # Call delete_topics to asynchronously delete topics, a future is returned.
        # By default this operation on the broker returns immediately while
        # topics are deleted in the background. But here we give it some time (30s)
        # to propagate in the cluster before returning.
        #
        # Returns a dict of <topic,future>.
        fs = self.admin_client.delete_topics(topics, operation_timeout=30)

        # Wait for operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                self.log_debug("Topic {} deleted".format(topic))
            except Exception as e:
                self.log_error("Failed to delete topic {}: {}".format(topic, e))

    def create_partitions(self, topic_partitions):
        """
        create partitions for list of topics
        :param topic_partitions: list of tuples (topic, number of partitions)
        :return:
        """

        new_parts = [
            NewPartitions(topic, int(new_total_count))
            for topic, new_total_count in topic_partitions
        ]

        # Try switching validate_only to True to only validate the operation
        # on the broker but not actually perform it.
        fs = self.admin_client.create_partitions(new_parts, validate_only=False)

        # Wait for operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                self.log_debug(
                    "Additional partitions created for topic {}".format(topic)
                )
            except Exception as e:
                self.log_error(
                    "Failed to add partitions to topic {}: {}".format(topic, e)
                )

    def list_topics(self, timeout: int = 10) -> list:
        """
        list topics and cluster metadata
        :param type: list topics or brokers or all; allowed values (all|topics|brokers)
        :param timeout: timeout in ms
        :return:
        """
        result = []
        md = self.admin_client.list_topics(timeout=timeout)

        for t in iter(md.topics.values()):
            result.append(str(t))

        return result
