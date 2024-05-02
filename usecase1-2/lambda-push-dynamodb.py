import json
import boto3
import os

def lambda_handler(event, context):
    # Parse the incoming event to get the S3 path
    event_body = json.loads(event['body'])
    s3_path = event_body.get('s3Path', '')
    bucket_name, s3_key = s3_path.split('/', 1)
    
    # Define function to read JSON data from S3
    def read_json_from_s3(bucket, key):
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read().decode('utf-8')
        return json.loads(file_content)
    
    # Define function to convert JSON data to DynamoDB format
    def convert_to_dynamodb_format(data, file_name):
        dynamodb_data = {'File_Name': {'S': file_name}}  # File name as partition key
        for key, value in data.items():
            if isinstance(value, str):
                dynamodb_data[key] = {'S': value}
            elif isinstance(value, (int, float)):
                dynamodb_data[key] = {'N': str(value)}
            # Add other data types as needed
        return dynamodb_data
    
    # Extract file name from the S3 key (without .json extension)
    file_name = os.path.splitext(os.path.basename(s3_key))[0]
    
    # Read JSON data from S3
    json_data = read_json_from_s3(bucket_name, s3_key)
    
    # Convert the JSON data to DynamoDB format with the file name as the partition key
    dynamodb_formatted_data = convert_to_dynamodb_format(json_data, file_name)
    
    # DynamoDB client
    dynamodb_client = boto3.client('dynamodb')
    
    # Specify your DynamoDB table name
    table_name = 'hui-billing-data'  # Replace with your table name
    
    # Put item in DynamoDB
    try:
        dynamodb_client.put_item(TableName=table_name, Item=dynamodb_formatted_data)
        print(f"Data pushed to DynamoDB with File Name: {file_name}")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Data pushed to DynamoDB with File Name: {file_name}"})
        }
    except Exception as e:
        print(f"Error pushing to DynamoDB: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
