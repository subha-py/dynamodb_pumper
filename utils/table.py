from utils.connect import connect
def create_table(table_name='auto-table-1'):
    client = connect()
    KeySchema = [{'AttributeName': 'username', 'KeyType': 'HASH'}, {'AttributeName': 'last_name', 'KeyType': 'RANGE'}]
    AttributeDefinitions = [{'AttributeName': 'username', 'AttributeType': 'S'}, {'AttributeName': 'last_name', 'AttributeType': 'S'}]

    ProvisionedThroughput = {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    response = client.create_table(TableName=table_name, KeySchema=KeySchema, AttributeDefinitions=AttributeDefinitions,
        ProvisionedThroughput=ProvisionedThroughput)