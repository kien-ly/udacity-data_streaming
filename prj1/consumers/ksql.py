import json
import logging
import requests
import topic_check

# Setup logger for detailed error and info tracking
logger = logging.getLogger(__name__)

# Define the KSQL URL endpoint for interaction
KSQL_URL = "http://localhost:8088"

# KSQL statement to create tables and summary for turnstile data
KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC = 'org.chicago.cta.station.turnstile.v1',  -- Use a static topic for all stations
    VALUE_FORMAT = 'avro',
    KEY = 'station_id',
    PARTITIONS = 6,  -- Increase the number of partitions to ensure better distribution of data in real-world scenarios
    REPLICAS = 3     -- Increase the number of replicas for better fault tolerance and data durability
);

CREATE TABLE turnstile_summary
WITH (VALUE_FORMAT = 'json') AS
    SELECT station_id, COUNT(station_id) AS count
    FROM turnstile
    GROUP BY station_id;
"""
# This KSQL statement will create the necessary tables and stream the data into a summary table.
# Ensure the turnstile topic is static (not changing per station) and partition/replica settings are optimized.

def execute_ksql_statement():
    """
    Executes the KSQL statement to create and configure the KSQL tables and streams.
    It checks if the summary table already exists to avoid re-creating it.
    """
    # Check if the 'TURNSTILE_SUMMARY' topic already exists to avoid duplication
    if topic_check.topic_exists("TURNSTILE_SUMMARY"):
        logger.info("Turnstile summary table already exists. Skipping execution.")
        return

    logger.debug("Executing KSQL statement to create turnstile and summary tables...")

    try:
        # Send the KSQL query to the KSQL server via HTTP POST
        response = requests.post(
            f"{KSQL_URL}/ksql",
            headers={"Content-Type": "application/vnd.ksql.v1+json"},
            data=json.dumps({
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"}
            }),
        )

        # Raise an exception if the HTTP request wasn't successful
        response.raise_for_status()
        logger.info("KSQL statement executed successfully.")

    except requests.exceptions.RequestException as e:
        # Log any error that occurs during the request
        logger.error(f"Error executing KSQL statement: {e}")
        raise  # Re-raise the exception to handle it further up if necessary

if __name__ == "__main__":
    # Run the KSQL statement execution when the script is run directly
    execute_ksql_statement()
