import logging
from pathlib import Path
from confluent_kafka import avro
from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware

# Logger setup to track events related to turnstile data
logger = logging.getLogger(__name__)

class Turnstile(Producer):
    """
    Creates a turnstile data producer to simulate and produce turnstile data for a specific station.
    The data is published to Kafka.
    """

    # Load Avro schemas for Kafka messages
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """
        Initializes the Turnstile producer for the given station.

        Args:
            station (Station): The station object associated with the turnstile.
        """
        # Sanitize the station name to be used in Kafka topic name
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        # Initialize the producer (inherited from Producer class)
        super().__init__(
            topic_name=f"org.chicago.cta.station.turnstile.v1.{station_name}",
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=3,
            num_replicas=1,
        )

        # Store the station and turnstile hardware references
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """
        Simulates riders entering through the turnstile at a given timestamp and time step.
        Publishes the entries to Kafka.

        Args:
            timestamp (int): The timestamp of the data being processed.
            time_step (int): The time step that defines the period of data simulation.
        """
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)

        # Produce Kafka messages for each entry detected by the turnstile hardware
        for _ in range(num_entries):
            try:
                self.producer.produce(
                    topic=self.topic_name,
                    key_schema=self.key_schema,
                    key={"timestamp": self.time_millis()},
                    value_schema=self.value_schema,
                    value={
                        "station_id": self.station.station_id,
                        "station_name": self.station.name,
                        "line": self.station.color.name
                    },
                )
            except Exception as e:
                # Log the error and raise the exception
                logger.critical(f"Error producing turnstile data: {e}")
                raise e
