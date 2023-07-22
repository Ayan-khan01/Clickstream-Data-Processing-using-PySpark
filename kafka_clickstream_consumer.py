from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType
from kafka import KafkaConsumer
import json

# Spark configuration
spark = SparkSession.builder.master("local[*]").appName("ClickstreamDataProcessor").getOrCreate()

# Kafka configuration
kafka_broker = 'localhost:9092'  # Replace with your Kafka broker's address
kafka_topic = 'clickstream_topic_name'  # Replace with your Kafka topic name

# Create a Kafka consumer instance
consumer = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_broker,
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))

def ingest_data():
    """
    Ingest clickstream data from Kafka.
    Deserialize the incoming messages from Kafka into a suitable format.

    Returns:
        generator: A generator that yields each clickstream data as a dictionary.
    """
    for message in consumer:
        try:
            clickstream_data = message.value
            yield clickstream_data
        except Exception as e:
