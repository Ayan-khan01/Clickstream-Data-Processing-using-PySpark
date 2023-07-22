import pandas as pd
import boto3
from elasticsearch import Elasticsearch

# Elasticsearch configuration
es_host = 'localhost'  # Replace with your Elasticsearch server's address
es_port = 9200  # Replace with your Elasticsearch server's port number (default is 9200)
es_index = 'clickstream_index'  # Replace with your Elasticsearch index name

# AWS S3 configuration
aws_access_key = 'YOUR_AWS_ACCESS_KEY'
aws_secret_key = 'YOUR_AWS_SECRET_KEY'
s3_bucket = 'YOUR_S3_BUCKET_NAME'
s3_key_prefix = 'clickstream_data/'  # Provide a prefix for easy search

def index_data_to_elasticsearch(url, country, avg_time_spent, num_clicks, num_unique_users):
    """
    Index the processed clickstream data into Elasticsearch.

    Parameters:
        url (str): The URL of the clicked page.
        country (str): The user's country.
        avg_time_spent (float): The average time spent on the URL by users from the country.
        num_clicks (int): The number of clicks on the URL by users from the country.
        num_unique_users (int): The number of unique users who clicked on the URL from the country.

    Returns:
        None
    """
    es = Elasticsearch([{'host': es_host, 'port': es_port}])

    doc = {
        'url': url,
        'country': country,
        'avg_time_spent': avg_time_spent,
        'num_clicks': num_clicks,
        'num_unique_users': num_unique_users
    }

    # Use the `index` method to index the document
    es.index(index=es_index, doc_type='_doc', body=doc)

def store_data_to_s3(processed_data):
    """
    Store the processed clickstream data into AWS S3 in CSV format.

    Parameters:
        processed_data (list): The list of processed clickstream data.

    Returns:
        None
    """
    # Convert the processed data to a DataFrame
    df = pd.DataFrame(processed_data)

    # Convert the DataFrame to a CSV file in memory
    csv_buffer = df.to_csv(index=False)

    # Upload the CSV file to AWS S3
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    s3_key = f"{s3_key_prefix}clickstream_data.csv"
    s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer)

if __name__ == "__main__":
    # Assuming you have a `processed_data` list of dictionaries (as mentioned in the comments)
    processed_data = [
        {
            'url': '/home',
            'country': 'US',
            'avg_time_spent': 25.5,
            'num_clicks': 100,
            'num_unique_users': 75
        },
        # Add more processed data rows as needed
    ]

    # Index each row of processed data to Elasticsearch
    for row in processed_data:
        index_data_to_elasticsearch(
            row['url'],
            row['country'],
            row['avg_time_spent'],
            row['num_clicks'],
            row['num_unique_users']
        )

    # Store the processed data to AWS S3 in CSV format
    store_data_to_s3(processed_data)
