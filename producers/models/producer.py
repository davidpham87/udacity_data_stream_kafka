"""Producer base-class providing common utilites and functionality"""

## (setq conda-anaconda-home "/usr/lib/miniconda3") ;; depends on where the installation is
## (setq python-shell-interpreter "ipython3")
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

BROKER_URLS = ','.join(["PLAINTEXT://localhost:9092","PLAINTEXT://localhost:9093","PLAINTEXT://localhost:9094"])
SCHEMA_REGISTRY_URL = "http://localhost:8081/"

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])
    timeout_ms = 1000

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": BROKER_URLS,
            "client.id": 1,
            "compression.type": "lz4"
        }

        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties,
            schema_registry=SCHEMA_REGISTRY_URL)


    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        topic_name = self.topic_name
        num_partitions = self.num_partitions
        num_replicas = self.num_replicas

        client = AdminClient(self.broker_properties)

        futures = client.create_topics(
            [NewTopic(topic=topic_name,
                      num_partitions=num_partitions,
                      replication_factor=num_replicas,
                      config={"cleanup.policy": "delete",
                              "compression.type": "lz4",
                              "delete.retention.ms": "2000",
                              "file.delete.delay.ms": "2000"})]
        )

        for topic, future in futures.items():
            try:
                future.result()
                print("topic created")
            except Exception as e:
                print(f"failed to create topic {topic_name}: {e}")

        logger.info("topic creation kafka integration")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush(self.timeout_ms)
        self.producer.close(timeout_ms)

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
