import logging
import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen

# Logger setup
logger = logging.getLogger(__name__)

# Kafka and Schema Registry URLs
BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


class KafkaConsumer:
    """
    A Kafka consumer that can handle both regular and Avro messages.
    This class supports asynchronous message consumption with Tornado.
    """

    def __init__(self, topic_name_pattern, message_handler, is_avro=True, offset_earliest=False, sleep_secs=1.0, consume_timeout=0.1):
        """
        Initializes the KafkaConsumer.

        Args:
            topic_name_pattern (str): The Kafka topic to subscribe to.
            message_handler (function): A handler function to process consumed messages.
            is_avro (bool): Whether the consumer should handle Avro data. Defaults to True.
            offset_earliest (bool): Whether to consume from the earliest offset. Defaults to False.
            sleep_secs (float): Sleep time between consume attempts in seconds. Defaults to 1.0.
            consume_timeout (float): Timeout for the consume operation. Defaults to 0.1 seconds.
        """
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        # Setting Kafka broker properties
        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "group.id": topic_name_pattern,
            "default.topic.config": {
                "auto.offset.reset": "earliest" if offset_earliest else "latest"
            },
        }

        # Choose the appropriate consumer based on Avro or regular Kafka
        if is_avro:
            self.broker_properties["schema.registry.url"] = SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        # Subscribe to the given topic
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """
        Callback for when Kafka assigns partitions to the consumer.
        Sets the offset to the earliest if specified.
        
        Args:
            consumer (Consumer): The Kafka consumer instance.
            partitions (list): List of partitions assigned to the consumer.
        """
        logger.info("Partitions assigned to consumer.")
        for partition in partitions:
            if self.offset_earliest:
                partition.offset = confluent_kafka.OFFSET_BEGINNING

        consumer.assign(partitions)

    async def consume(self):
        """
        Asynchronously consumes messages from the Kafka topic.

        This method loops indefinitely, polling for messages and processing them using the provided handler.
        """
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """
        Polls for messages and processes them.

        Returns:
            int: 1 if a message was received and processed, 0 otherwise.
        """
        try:
            msg = self.consumer.poll(timeout=self.consume_timeout)
        except Exception as e:
            logger.error(f"Exception occurred while polling from {self.topic_name_pattern}: {e}")
            return 0

        if msg is None:
            return 0
        
        if msg.error():
            logger.error(f"Error while consuming from {self.topic_name_pattern}: {msg.error()}")
            return 0

        # Handle the message using the provided message handler
        self.message_handler(msg)
        return 1

    def close(self):
        """
        Closes the Kafka consumer and cleans up any resources.
        """
        self.consumer.close()
        logger.info(f"Consumer for {self.topic_name_pattern} closed.")
