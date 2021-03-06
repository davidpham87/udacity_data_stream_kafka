"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)

BROKER_URLS = ','.join(["PLAINTEXT://localhost:9092",
                        "PLAINTEXT://localhost:9093",
                        "PLAINTEXT://localhost:9094"])
SCHEMA_REGISTRY_URL = "http://localhost:8081"


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        brocker_properties = {
            "bootstrap.servers": BROKER_URLS,
            "group.id": topic_name_pattern,
            "compression.type": "lz4"}

        if self.offset_earliest:
            brocker_properties["default.topic.config"] = {"auto.offset.reset": "earliest"}

        self.broker_properties = brocker_properties

        if is_avro is True:
            self.broker_properties["schema.registry.url"] = SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        for partition in partitions:
            if self.offset_earliest:
                partition.offset = confluent_kafka.OFFSET_BEGINNING
        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        return_value = 0
        message = None
        try:
            message = self.consumer.poll(timeout=self.consume_timeout)
        except SerializerError as e:
            logging.error(f"Failed deserialization. topic and error: %s, %s", self.topic_name_pattern, e)
        if message is None:
            return return_value

        if message.error() is not None:
            logging.error(f"Messag error. Topic and Message: %s, %s",
                          self.topic_name_pattern, message.error())

        if message and not message.error():
            try:
                self.message_handler(message)
                return_value = 1
            except:
                logging.error(f"Processing Error. Topic and Message: %s, %s",
                              self.topic_name_pattern, message)

        return return_value


    def close(self):
        """Cleans up any open kafka consumers"""
        logging.debug("Closing consumer.")
        self.consumer.close()
