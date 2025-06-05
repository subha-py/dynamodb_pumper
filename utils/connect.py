import boto3

def connect():
    return boto3.resource('dynamodb', region_name='us-east-1')




if __name__ == '__main__':
    client = connect()