from pyspark.sql import SparkSession

# Spark configuration
spark = SparkSession.builder.master("local[*]").appName("ClickstreamDataProcessor").getOrCreate()

def process_clickstream_data(clickstream_data):
    """
    Process the clickstream data received from HBase.

    Parameters:
        clickstream_data (dict): The clickstream data in dictionary format.

    Returns:
        tuple: A tuple containing URL, country, and time_spent.
    """
    # Handling null values: Replace null with "NA"
    url = clickstream_data['click_data']['url'] or "NA"
    country = clickstream_data['geo_data']['country'] or "NA"
    time_spent = clickstream_data['click_data'].get('time_spent', 0)  # Using get() method to handle missing key

    return url, country, time_spent
   
    # Develop your logic to process the clickstream data

def process_stored_data():
    """
    Periodically process the stored clickstream data from HBase using Apache Spark.

    Returns:
        None
    """
    connection = happybase.Connection(hbase_host)
    table = connection.table(hbase_table_name)
    
    # Count null values before importing
    null_count_before = table.count(empty=True)

    rows = [data for key, data in table.scan()]

    # Convert the HBase data to a Spark DataFrame
    spark_df = spark.createDataFrame(rows)

    # Register the DataFrame as a temporary table for Spark SQL queries
    spark_df.createOrReplaceTempView("clickstream_table")

    # Execute Spark SQL queries for data processing and aggregation
    result = spark.sql("""
        SELECT
            click_data.url AS url,
            geo_data.country AS country,
            AVG(click_data.time_spent) AS avg_time_spent,
            COUNT(click_data.url) AS num_clicks,
            COUNT(DISTINCT click_data.user_id) AS num_unique_users
        FROM
            clickstream_table
        GROUP BY
            click_data.url,
            geo_data.country
    """)

    # Convert the Spark DataFrame to a Python list for further processing
    processed_data = result.collect()

    # Count null values after importing
    null_count_after = table.count(empty=True)

    for row in processed_data:
        url = row['url']
        country = row['country']
        avg_time_spent = row['avg_time_spent']
        num_clicks = row['num_clicks']
        num_unique_users = row['num_unique_users']

        # Index the processed data into Elasticsearch in the subsequent step
        pass

    # Print the null count before and after importing
    print("Null count before importing:", null_count_before)
    print("Null count after importing:", null_count_after)

if __name__ == "__main__":
    process_stored_data()
