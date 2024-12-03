import logging
from pathlib import Path
from confluent_kafka import avro
from models import Turnstile
from models.producer import Producer

# Logger setup to track events related to Station
logger = logging.getLogger(__name__)

class Station(Producer):
    """
    Defines a station for the CTA L (Chicago Transit Authority) system, managing arrivals
    of trains and interactions with turnstile data.
    """

    # Load Avro schemas for Kafka messages
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_value.json")

    def __init__(self, station_id, name, color, direction_a=None, direction_b=None):
        """
        Initializes the Station instance.

        Args:
            station_id (int): The unique identifier of the station.
            name (str): The name of the station.
            color (str): The color of the line this station belongs to.
            direction_a (Direction, optional): The direction A (optional).
            direction_b (Direction, optional): The direction B (optional).
        """
        # Prepare the station name and Kafka topic name by sanitizing the station name
        station_name = (
            name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        topic_name = f"org.chicago.cta.{station_name}.arrivals.v1"

        # Initialize the producer (inherited from Producer class)
        super().__init__(
            topic_name,
            key_schema=Station.key_schema,
            value_schema=Station.value_schema,
            num_partitions=1,
            num_replicas=1,
        )

        # Station attributes
        self.station_id = int(station_id)
        self.name = name
        self.color = color
        self.dir_a = direction_a
        self.dir_b = direction_b
        self.a_train = None
        self.b_train = None
        self.turnstile = Turnstile(self)  # Initialize Turnstile for this station

    def run(self, train, direction, prev_station_id, prev_direction):
        """
        Simulates train arrivals at this station and sends data to Kafka.

        Args:
            train (Train): The train object arriving at the station.
            direction (str): The direction of the train ('a' or 'b').
            prev_station_id (int): The station ID the train came from.
            prev_direction (str): The direction the train came from.
        """
        logger.info("Arrival Kafka integration incomplete.")
        try:
            # Produce a Kafka message for the train arrival
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": self.time_millis()},
                key_schema=self.key_schema,
                value_schema=self.value_schema,
                value={
                    'station_id': self.station_id,
                    'train_id': train.train_id,
                    'direction': direction,
                    'line': self.color.name,
                    'train_status': train.status.name,
                    'prev_station_id': prev_station_id,
                    'prev_direction': prev_direction
                },
            )
        except Exception as e:
            # Log the exception and raise it
            logger.critical(f"Error producing message: {e}")
            raise e

    def __str__(self):
        """
        Returns a human-readable string representation of the station's current status.
        """
        return "Station | {:^5} | {:<30} | Direction A: | {:^5} | departing to {:<30} | Direction B: | {:^5} | departing to {:<30} | ".format(
            self.station_id,
            self.name,
            self.a_train.train_id if self.a_train else "---",
            self.dir_a.name if self.dir_a else "---",
            self.b_train.train_id if self.b_train else "---",
            self.dir_b.name if self.dir_b else "---",
        )

    def __repr__(self):
        """
        Returns a string representation of the station object (for debugging purposes).
        """
        return str(self)

    def arrive_a(self, train, prev_station_id, prev_direction):
        """
        Marks the arrival of a train at the station from direction 'a'.

        Args:
            train (Train): The arriving train.
            prev_station_id (int): The ID of the previous station.
            prev_direction (str): The direction the train came from.
        """
        self.a_train = train
        self.run(train, "a", prev_station_id, prev_direction)

    def arrive_b(self, train, prev_station_id, prev_direction):
        """
        Marks the arrival of a train at the station from direction 'b'.

        Args:
            train (Train): The arriving train.
            prev_station_id (int): The ID of the previous station.
            prev_direction (str): The direction the train came from.
        """
        self.b_train = train
        self.run(train, "b", prev_station_id, prev_direction)

    def close(self):
        """
        Cleans up resources, including closing the turnstile and producer.
        """
        self.turnstile.close()
        super(Station, self).close()
