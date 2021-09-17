"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            'bootstrap.servers': "PLAINTEXT://localhost:9092",
            'schema.registry.url': "http://localhost:8081"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(config=self.broker_properties,
                                     default_key_schema=self.value_schema,
                                     default_value_schema=self.value_schema)

    @staticmethod
    def topic_exists(client, topic_name):
        """Checks if the given topic exists"""
        topics = client.list_topics().topics

        return topics.get(topic_name) is not None

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        # Get list of existing topics and check if already exists
        client = AdminClient({"bootstrap.servers": self.broker_properties['bootstrap.servers']})
        exists = self.topic_exists(client, self.topic_name)

        if exists:
            logging.debug(f"Topic {self.topic_name} already exists, skipping creation")
            return

        futures = client.create_topics(
            [NewTopic(topic=self.topic_name,
                      num_partitions=self.num_partitions,
                      replication_factor=self.num_replicas,
                      config={'cleanup.policy': 'delete',
                              'compression.type': 'lz4',
                              'delete.retention.ms': 2000,
                              'file.delete.delay.ms': 2000})])

        for topic, future in futures.items():
            try:
                future.result()
                logging.debug(f"topic created: {self.topic_name}")
            except Exception as e:
                logging.error(f"failed to create topic {self.topic_name}: {e}")
                raise

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()

    @staticmethod
    def time_millis():
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
