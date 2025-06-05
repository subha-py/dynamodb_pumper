from utils.connect import connect
def create_table(table_name='auto-table-1'):
    client = connect()
    KeySchema = [{'AttributeName': 'username', 'KeyType': 'HASH'}, {'AttributeName': 'last_name', 'KeyType': 'RANGE'}]
    AttributeDefinitions = [{'AttributeName': 'username', 'AttributeType': 'S'}, {'AttributeName': 'last_name', 'AttributeType': 'S'}]
    try:
        response = client.create_table(TableName=table_name, KeySchema=KeySchema, AttributeDefinitions=AttributeDefinitions,
            BillingMode='PAY_PER_REQUEST')
        response.wait_until_exists()
    except Exception as e:
        if 'Table already exists' in str(e):
            print('Table already exist')
            pass

def get_table(table_name='auto-table-1'):
    client = connect()
    return client.Table(table_name)

if __name__ == '__main__':
    create_table()