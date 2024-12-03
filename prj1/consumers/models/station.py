import json
import logging

# Logger setup for tracking station-related events
logger = logging.getLogger(__name__)

class Station:
    """
    Represents a station with attributes like its ID, name, order,
    and the current status of trains in two directions (a and b).
    """

    def __init__(self, station_id, station_name, order):
        """
        Initializes the Station instance.

        Args:
            station_id (int): The unique identifier of the station.
            station_name (str): The name of the station.
            order (int): The position/order of the station in the system.
        """
        self.station_id = station_id
        self.station_name = station_name
        self.order = order
        self.dir_a = None  # Direction A status (for trains coming in this direction)
        self.dir_b = None  # Direction B status (for trains coming in this direction)
        self.num_turnstile_entries = 0  # Tracks the number of turnstile entries

    @classmethod
    def from_message(cls, value):
        """
        Creates a Station instance from a Kafka message.

        Args:
            value (dict): The data extracted from the Kafka message.

        Returns:
            Station: A Station instance.
        """
        return cls(value["station_id"], value["station_name"], value["order"])

    def handle_departure(self, direction):
        """
        Handles the departure of a train by clearing its data for the given direction.

        Args:
            direction (str): The direction the train is departing from, 'a' or 'b'.
        """
        if direction == "a":
            self.dir_a = None  # Clear the train data for direction A
        else:
            self.dir_b = None  # Clear the train data for direction B

    def handle_arrival(self, direction, train_id, train_status):
        """
        Handles the arrival of a train by storing its ID and status for the given direction.

        Args:
            direction (str): The direction the train is arriving from, 'a' or 'b'.
            train_id (str): The unique ID of the arriving train.
            train_status (str): The current status of the arriving train.
        """
        status_dict = {
            "train_id": train_id,
            "status": train_status.replace("_", " ")  # Clean up train status for readability
        }
        if direction == "a":
            self.dir_a = status_dict  # Store arrival details for direction A
        else:
            self.dir_b = status_dict  # Store arrival details for direction B

    def process_message(self, json_data):
        """
        Processes turnstile data to update the number of turnstile entries at the station.

        Args:
            json_data (dict): The JSON data containing turnstile information.
        """
        self.num_turnstile_entries = json_data.get("COUNT", 0)  # Update turnstile count
