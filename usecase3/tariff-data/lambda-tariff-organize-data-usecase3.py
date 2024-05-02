import json
import boto3

# # Specify your S3 bucket details
# s3_bucket_name = "bill-compare-hui"

# body_content = json.loads(event['body'])
# output_s3_path = body_content['output_s3_path']
# object_key = output_s3_path.split('/', 1)

# s3_input_object_key = object_key
# s3_output_object_key = "tariff-organize-json/dynamodb_formatted.json"  # The S3 object key for the output file
# dynamodb_table_name = "hui-tariff-data-v2"  # The DynamoDB table name

# # Create an S3 client using the default AWS credentials provider chain
# s3_client = boto3.client("s3")

# # Create a DynamoDB client using the default AWS credentials provider chain
# dynamodb_client = boto3.client("dynamodb")

# Your original script logic wrapped in a function
def read_transform_upload_data(s3_client, dynamodb_client, s3_bucket_name, s3_input_object_key, s3_output_object_key, dynamodb_table_name):
    try:
        # Download the data from S3
        response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_input_object_key)
        data = response["Body"].read().decode("utf-8")
        
        # Parse the data as JSON
        input_data = json.loads(data)
        
        # Perform the transformation
        dynamodb_data = {"TableName": dynamodb_table_name, "Items": []}
        for item in input_data:
            if "Unnamed: 0" in item:
                # This is a table header, skip it
                continue
            
            dynamodb_item = {}
            
            # Check if "Tariff code" is present in the item
            if "Unnamed: 1" not in item:
                continue

            # Define a mapping for "Unnamed" attributes to the desired attribute names
            attribute_mapping = {
                "Unnamed: 0": "Tariff class",
                "Unnamed: 1": "Tariff code",
                "Unnamed: 2": "Tariff Structure",
                "Unnamed: 3": "Description",
                "Unnamed: 4": "Closed to New Entrants",
                "Unnamed: 5": "Standing charge $/year",
                "Unnamed: 7": "Block 1 c/kWh",
                "Unnamed: 8": "Block 2 c/kWh",
                "Unnamed: 9": "Peak c/kWh",
                "Unnamed: 10": "Shoulder all year c/kWh",
                "Unnamed: 11": "Summer peak c/kWh",
                "Unnamed: 12": "Summer shoulder c/kWh",
                "Unnamed: 13": "Winter peak c/kWh",
                "Unnamed: 14": "Off Peak c/kWh",
                "Unnamed: 15": "Dedicated circuit c/kWh",
                "Unnamed: 16": "Feed in rates c/kWh",
                "Unnamed: 17": "Capacity $/kVA/yr",
                "Unnamed: 18": "Critical peak demand $/kVA/yr",
                "Unnamed: 19": "Monthly peak kW demand $/kW/mth",
                "Unnamed: 20": "Monthly off peak kW demand $/kW/mth"
            }
    
            for key, value in item.items():
                if key in attribute_mapping:
                    attribute_name = attribute_mapping[key]
                    # Set "Tariff code," "Closed to New Entrants," and "Description" as strings (S), others as numbers (N)
                    data_type = "S" if attribute_name in ["Tariff code", "Closed to New Entrants", "Description"] else "N"
                    dynamodb_item[attribute_name] = value["S"]
            
            dynamodb_data["Items"].append(dynamodb_item)
        
        # Upload the DynamoDB-formatted data to S3
        s3_client.put_object(
            Bucket=s3_bucket_name,
            Key=s3_output_object_key,
            Body=json.dumps(dynamodb_data, indent=2),
            ContentType="application/json"
        )

        print("Data has been transformed and uploaded to S3.")
    except Exception as e:
        print(f"Error reading, transforming, and uploading data: {str(e)}")

# Lambda handler function
def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    dynamodb_client = boto3.client("dynamodb")

    # Extract 'body' content from the event
    if 'body' in event:
        body_content = json.loads(event['body'])
    else:
        return {
            "statusCode": 400,
            "body": json.dumps("Event body is missing")
        }
    
    # Extract 'output_s3_path' from the body_content
    if 'output_s3_path' in body_content:
        output_s3_path = body_content['output_s3_path']
    else:
        return {
            "statusCode": 400,
            "body": json.dumps("output_s3_path is missing in the event body")
        }

    # Parse the output_s3_path to extract bucket name and object key
    s3_bucket_name = "bill-compare-hui"
    _, s3_input_object_key = output_s3_path.split('/', 1)  # Assuming the bucket name is not needed as it's known

    # Specify the output object key and DynamoDB table name
    s3_output_object_key = "tariff-organize-json/dynamodb_formatted.json"
    dynamodb_table_name = "hui-tariff-data"
    
    # Invoke the data processing and uploading function
    try:
        result = read_transform_upload_data(s3_client, dynamodb_client, s3_bucket_name, s3_input_object_key, s3_output_object_key, dynamodb_table_name)
        return {
            "statusCode": 200,
            "body": json.dumps(result)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error processing data: {str(e)}")
        }

