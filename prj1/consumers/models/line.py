import json
import logging
from models import Station

# Logger setup for tracking issues and process flow
logger = logging.getLogger(__name__)

class Line:
    """Represents a line in the system with its stations and functionalities."""

    def __init__(self, color):
        """
        Initializes the Line with a color and corresponding color code.
        
        Args:
            color (str): The color of the line, can be 'blue', 'red', or 'green'.
        """
        self.color = color
        self.color_code = self._get_color_code(color)
        self.stations = {}

    def _get_color_code(self, color):
        """
        Returns the color code based on the line color.
        
        Args:
            color (str): The color of the line (e.g., 'blue', 'red', 'green').
        
        Returns:
            str: The hex color code for the line.
        """
        color_map = {
            "blue": "#1E90FF",
            "red": "#DC143C",
            "green": "#32CD32"
        }
        return color_map.get(color, "0xFFFFFF")  # Default to white if no color match

    def _handle_station(self, station_data):
        """
        Adds a station to the current line's station list based on the station data.
        
        Args:
            station_data (dict): The data for the station being added to the line.
        """
        if station_data["line"] != self.color:
            return
        self.stations[station_data["station_id"]] = Station.from_message(station_data)

    def _handle_arrival(self, message):
        """
        Updates the station's data when a train arrives, including handling departures.
        
        Args:
            message (KafkaMessage): The Kafka message containing train arrival data.
        """
        value = message.value()
        prev_station_id = value.get("prev_station_id")
        prev_dir = value.get("prev_direction")
        
        if prev_station_id and prev_dir:
            prev_station = self.stations.get(prev_station_id)
            if prev_station:
                prev_station.handle_departure(prev_dir)
            else:
                logger.debug("Unable to handle previous station due to missing station.")
        else:
            logger.debug("Previous station information is missing; skipping departure handling.")

        # Handle current station arrival
        station_id = value.get("station_id")
        station = self.stations.get(station_id)
        
        if station:
            station.handle_arrival(value.get("direction"), value.get("train_id"), value.get("train_status"))
        else:
            logger.debug(f"Unable to handle message due to missing station (ID: {station_id})")

    def process_message(self, message):
        """
        Processes incoming Kafka messages based on their topic and updates the line's data.
        
        Args:
            message (KafkaMessage): The Kafka message to process.
        """
        topic = message.topic()

        if 'stations.table' in topic:
            # Handle station updates from the stations table
            self._process_station_update(message)
        elif 'arrival' in topic:
            # Handle train arrival updates
            self._handle_arrival(message)
        elif 'TURNSTILE_SUMMARY' in topic:
            # Handle turnstile summary data
            self._process_turnstile_summary(message)
        else:
            logger.debug(f"Unable to find handler for message from topic {topic}.")

    def _process_station_update(self, message):
        """
        Processes updates for stations and adds them to the line's station list.
        
        Args:
            message (KafkaMessage): The Kafka message containing station data.
        """
        try:
            station_data = json.loads(message.value())
            self._handle_station(station_data)
        except Exception as e:
            logger.fatal(f"Error processing station data: {e}")

    def _process_turnstile_summary(self, message):
        """
        Processes turnstile summary data and updates station information accordingly.
        
        Args:
            message (KafkaMessage): The Kafka message containing turnstile summary data.
        """
        try:
            json_data = json.loads(message.value())
            station_id = json_data.get("STATION_ID")
            station = self.stations.get(station_id)
            if station:
                station.process_message(json_data)
            else:
                logger.debug(f"Unable to process turnstile summary for missing station (ID: {station_id}).")
        except Exception as e:
            logger.error(f"Error processing turnstile summary: {e}")
