import logging
import json

# Logger setup for tracking weather-related events
logger = logging.getLogger(__name__)

class Weather:
    """
    Represents the Weather model which includes information like temperature and weather status.
    """

    def __init__(self):
        """
        Initializes the Weather model with default values for temperature and weather status.
        """
        self.temperature = 70.0  # Default temperature in Fahrenheit
        self.status = "sunny"  # Default weather status

    def process_message(self, message):
        """
        Processes incoming weather data and updates the model's temperature and status.

        Args:
            message (KafkaMessage): The Kafka message containing weather data.
        """
        try:
            # Parse the incoming message (assumed to be in JSON format)
            data = json.loads(message.value())

            # Safely extract and update temperature and status from the incoming data
            self.temperature = data.get('temperature', self.temperature)  # Use current value if missing
            self.status = data.get('status', self.status)  # Use current value if missing

            logger.info(f"Weather updated: {self.status} with temperature {self.temperature}°F")
        except json.JSONDecodeError as e:
            # Log an error if the message is not valid JSON
            logger.error(f"Failed to decode weather message: {e}")
        except KeyError as e:
            # Log a warning if the expected keys are missing in the message
            logger.warning(f"Missing key in weather data: {e}")
        except Exception as e:
            # Log any other unexpected errors
            logger.error(f"Unexpected error while processing weather message: {e}")
