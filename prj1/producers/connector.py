import json
import logging
import requests

# Logger setup for tracking Kafka Connector related events
logger = logging.getLogger(__name__)

# Kafka Connect URL and Connector Name
KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """
    Starts and configures the Kafka Connect connector for the Postgres 'stations' table.
    If the connector already exists, it skips the creation step.
    """
    logger.debug("Checking if Kafka Connect connector already exists...")

    # Check if the connector already exists
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    
    if resp.status_code == 200:
        logger.debug("Connector already exists. Skipping creation.")
        return  # Skip creation if connector already exists

    # Kafka Connect configuration for JDBC Source Connector to connect to Postgres
    logger.debug("Creating new Kafka Connect connector...")
    
    # Define the connector configuration payload
    connector_config = {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "batch.max.rows": "500",
            "connection.url": "jdbc:postgresql://localhost:5432/cta",
            "connection.user": "cta_admin",
            "connection.password": "chicago",
            "table.whitelist": "stations",  # Specify the table to be ingested
            "mode": "incrementing",  # Use incrementing mode for fetching new rows
            "incrementing.column.name": "stop_id",  # Increment based on stop_id
            "topic.prefix": "postgres-cta-",  # Prefix for Kafka topics
            "poll.interval.ms": "300000",  # Poll interval of 5 minutes (300000 ms)
        }
    }

    # Send a POST request to create the connector
    try:
        resp = requests.post(
            KAFKA_CONNECT_URL,
            headers={"Content-Type": "application/json"},
            data=json.dumps(connector_config)
        )
        
        # Ensure the request was successful
        resp.raise_for_status()  # Raise an exception if status code is not 2xx
        logger.debug("Connector created successfully.")

    except requests.exceptions.RequestException as e:
        # Log any request exceptions (e.g., connection issues, invalid response)
        logger.error(f"Failed to create or configure the connector: {e}")
        raise  # Re-raise the exception to handle it further up if necessary

if __name__ == "__main__":
    # Run the connector configuration when the script is executed
    configure_connector()
