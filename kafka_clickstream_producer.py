from kafka import KafkaProducer
import json
import time
import random

# Kafka configuration
kafka_broker = 'localhost:9092'  # Replace with your Kafka broker's address
kafka_topic = 'clickstream_topic'  # Replace with your Kafka topic name

# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=kafka_broker,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Dummy data for generating clickstream data
user_ids = ['user1', 'user2', 'user3']
urls = ['/home', '/products', '/about', '/contact']
countries = ['US', 'UK', 'Canada', 'Germany']
cities = ['New York', 'London', 'Toronto', 'Berlin']
browsers = ['Chrome', 'Firefox', 'Safari', 'Edge']
os = ['Windows', 'macOS', 'Linux', 'iOS']
devices = ['Desktop', 'Mobile']

def generate_dummy_clickstream_data():
    """
    Generate clickstream data in JSON format.

    Returns:
        dict: A dictionary containing the generated clickstream data.
    """
    user_id = random.choice(user_ids)
    url = random.choice(urls)
    country = random.choice(countries)
    city = random.choice(cities)
    browser = random.choice(browsers)
    operating_system = random.choice(os)
    device = random.choice(devices)
    timestamp = int(time.time())

    clickstream_data = {
        'user_id': user_id,
        'url': url,
        'country': country,
        'city': city,
        'browser': browser,
        'os': operating_system,
        'device': device,
        'timestamp': timestamp
    }

    return clickstream_data

if __name__ == "__main__":
    try:
        while True:
            clickstream_data = generate_dummy_clickstream_data()
            producer.send(kafka_topic, value=clickstream_data)
            print(f"Produced: {clickstream_data}")
            time.sleep(1)  # Delay between producing messages
    except KeyboardInterrupt:
        producer.close()
