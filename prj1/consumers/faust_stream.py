import logging
import faust

# Set up logger for better traceability
logger = logging.getLogger(__name__)

# Define the structure of incoming Kafka records (Station)
class Station(faust.Record):
    """
    Represents a Station record ingested from Kafka.
    """
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Define the structure of transformed station records that will be published to Kafka
class TransformedStation(faust.Record):
    """
    Represents a transformed Station record to be written back to Kafka with necessary fields.
    """
    station_id: int
    station_name: str
    order: int
    line: str  # Line color (red, blue, green)


# Faust application setup
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

# Define input Kafka Topic where raw station data is ingested
input_topic = app.topic("postgres-cta-stations", value_type=Station)

# Define output Kafka Topic where transformed station data will be sent
output_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)

# Faust Table to store transformed station records
station_table = app.Table(
    "org.chicago.cta.stations.table.v1",
    default=TransformedStation,
    partitions=1,
    changelog_topic=output_topic,
)


@app.agent(input_topic)
async def transform_station_data(stations):
    """
    Transforms incoming station data by assigning the appropriate line color
    and storing it in a Faust Table.
    """
    async for station in stations:
        # Determine the line based on the station flags (red, blue, green)
        if station.red:
            line_color = 'red'
        elif station.blue:
            line_color = 'blue'
        elif station.green:
            line_color = 'green'
        else:
            line_color = ''  # Default if no line is marked

        # Store the transformed station data into the Faust Table
        station_table[station.station_id] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line_color
        )


if __name__ == "__main__":
    # Run the Faust application
    app.main()
