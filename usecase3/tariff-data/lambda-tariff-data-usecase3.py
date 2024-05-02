import boto3
import pandas as pd
import io
import json

def lambda_handler(event, context):

    # Specify the S3 bucket name and the CSV object key
    # input_bucket_name = 'bill-compare-hui'
    # input_object_key = 'tariff-data/data.csv'
    input_bucket_name = event.get('bucket')
    input_object_key = event.get('key')
    if not input_bucket_name or not input_object_key:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "message": "Missing 'bucket' or 'key' in the Lambda function input."
            })
        }

    # Convert the CSV to DynamoDB JSON format
    dynamodb_json_data = read_csv_from_s3_and_prepare_dynamodb_json(input_bucket_name, input_object_key)
    
    # Specify the output S3 bucket name and object key for the DynamoDB JSON data
    output_bucket_name = input_bucket_name  # Assuming you're saving back to the same bucket
    output_object_key = 'tariff-data-json/data_dynamodb.json'
    
    # Save the DynamoDB JSON data to S3
    save_dynamodb_json_to_s3(dynamodb_json_data, output_bucket_name, output_object_key)


    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "CSV converted to DynamoDB JSON and saved.",
            "output_s3_path": f"{input_bucket_name}/{output_object_key}"
        })
    }

def read_csv_from_s3_and_prepare_dynamodb_json(bucket_name, object_key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=object_key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), dtype=str)

    dynamodb_json_items = []
    for index, row in df.iterrows():
        item = {}
        for column, value in row.items():
            if pd.notna(value):
                item[column] = {'S': value}
        dynamodb_json_items.append(item)

    return dynamodb_json_items

def save_dynamodb_json_to_s3(json_data, bucket_name, output_object_key):
    s3 = boto3.client('s3')
    json_string = json.dumps(json_data)
    s3.put_object(Bucket=bucket_name, Key=output_object_key, Body=json_string)
    print(f"DynamoDB JSON data saved to s3://{bucket_name}/{output_object_key}")
