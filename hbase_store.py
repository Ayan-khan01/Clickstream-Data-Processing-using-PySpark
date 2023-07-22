import happybase

# Required HBase configuration
hbase_host = 'localhost'  # Need to replace with your HBase server's address
hbase_table_name = 'clickstream_data'  # Replace with your HBase table name
hbase_column_family = {
    'click_data': {},
    'geo_data': {},
    'user_agent_data': {}
}

def store_data_to_hbase(clickstream_data):
    """
    Put the ingested clickstream data in HBase.

    Parameters:
        clickstream_data (dict): The clickstream data in dictionary format.

    Returns:
        None
    """
    connection = happybase.Connection(hbase_host)
    table = connection.table(hbase_table_name)

    row_key = clickstream_data['click_id']  # Replace 'click_id' with the actual key from your clickstream data

    # Use a batch to perform multiple puts in a single operation for better performance
    with table.batch() as batch:
        batch.put(row_key, clickstream_data['click_data'], timestamp=None, wal=True)
        batch.put(row_key, clickstream_data['geo_data'], timestamp=None, wal=True)
        batch.put(row_key, clickstream_data['user_agent_data'], timestamp=None, wal=True)

if __name__ == "__main__":
    for clickstream_data in ingest_data():  # Assuming you have a function named 'ingest_data' elsewhere that provides clickstream_data
        store_data_to_hbase(clickstream_data)
