import json
import boto3

# Initialize the boto3 clients for S3 and DynamoDB
s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')
bedrock_client = boto3.client('bedrock-runtime')

def lambda_handler(event, context):
    # Assuming 'bucket_name' and 'file_key' are provided in the Lambda event
    bucket_name = 'bill-compare-hui'
    file_key = 'energy-output-organized-results/CI_Invoice_10992378_aaah_account_details_organized.txt'
    
    # Read the file content from S3
    s3_response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    s3_content = s3_response['Body'].read().decode('utf-8')
    
    # Fetch the tariff data from DynamoDB
    dynamodb_response = dynamodb_client.get_item(
        TableName='hui-tariff-data',
        Key={
            'Tariff code': {'S': 'NEE12'},
            'uuid': {'S': '014f4704-60d3-4042-8784-f90d5f773182'}
        }
    )
    
    # Construct the prompt for the AI model
    prompt_data = f"Command: Compare the peak and off-peak data from the S3 billing data with the DynamoDB tariff data and provide a in depth summary of the matches and discrepancies that is including the invoice name and the tariff code.\n\n" \
                  f"Does the network tariff rates on the s3 data matches to dynamodb data.\n\n" \
                  f"**S3 Billing Data** (Peak and Off-Peak):\n{s3_content}\n\n" \
                  f"**DynamoDB Tariff Data**:\n{json.dumps(dynamodb_response.get('Item'))}\n"
    
    try:
        # Prepare the body for the API request
        body = json.dumps({"inputText": prompt_data})
        modelId = "amazon.titan-tg1-large"
        accept = "application/json"
        contentType = "application/json"
    
        # Invoke the Bedrock model
        response = bedrock_client.invoke_model(
            body=body, modelId=modelId, accept=accept, contentType=contentType
        )
        response_body = json.loads(response.get("body").read())
    
        # Instead of printing, return the AI model's response
        return {
            'statusCode': 200,
            'body': json.dumps(response_body.get("results")[0].get("outputText"))
        }

    except boto3.exceptions.Boto3Error as error:
        # Handle errors by returning them
        return {
            'statusCode': 500,
            'body': json.dumps(f"An error occurred: {error}")
        }
