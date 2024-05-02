import json
import boto3
import uuid  # Import the uuid library to generate UUIDs

def lambda_handler(event, context):
    # Specify your S3 and DynamoDB details
    s3_bucket_name = "bill-compare-hui"
    s3_input_object_key = "tariff-organize-json/dynamodb_formatted.json"  # The S3 object key for the input file
    dynamodb_table_name = "hui-tariff-data"  # The DynamoDB table name

    # Create an S3 client and DynamoDB resource using the default AWS credentials provider chain
    s3_client = boto3.client("s3")
    dynamodb_resource = boto3.resource("dynamodb")

    # Get the DynamoDB table
    table = dynamodb_resource.Table(dynamodb_table_name)

    try:
        # Download the data from S3
        response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_input_object_key)
        data = response["Body"].read().decode("utf-8")
        
        # Parse the data as JSON
        dynamodb_data = json.loads(data)
        
        # Iterate through the items and insert them into DynamoDB
        for item in dynamodb_data["Items"]:
            # Generate a new UUID for each item as the sort key
            item["uuid"] = str(uuid.uuid4())
            
            # Use the put_item method to insert the item into DynamoDB
            table.put_item(Item=item)
        
        print(f"Data inserted into DynamoDB table '{dynamodb_table_name}' successfully.")
    except Exception as e:
        print(f"Error reading and pushing data to DynamoDB: {str(e)}")

