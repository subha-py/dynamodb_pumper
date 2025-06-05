import boto3

def connect():
    return boto3.client('dynamodb', region_name='us-east-1')




