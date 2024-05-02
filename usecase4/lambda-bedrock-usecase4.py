import boto3
import json

# Initialize the boto3 S3 client
s3_client = boto3.client('s3')

# Initialize the boto3 Bedrock Runtime client
bedrock_client = boto3.client('bedrock-runtime')

def lambda_handler(event, context):
    def get_json_data_from_s3(bucket, key):
        # Retrieve the JSON file from S3
        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            json_content = response['Body'].read().decode('utf-8')
            return json.loads(json_content)
        except Exception as e:
            return {'error': f"Error getting data from S3: {e}"}

    def invoke_bedrock_model(input_text):
        try:
            # Construct the Bedrock API request payload
            payload = {"inputText": input_text}
            
            # Use the AWS SDK to make the Bedrock API call
            response = bedrock_client.invoke_endpoint(
                EndpointName="amazon.titan-embed-text-v1",  
                ContentType="application/json",
                Body=json.dumps(payload),
                Accept="application/json"
            )
            
            # Read the response content from the StreamingBody
            response_content = json.loads(response['Body'].read())
            return response_content
        except Exception as e:
            return {'error': f"Error invoking Bedrock model: {e}"}

    def process_invoice_data(invoice_json_data):
        # Initialize variables
        total_usage = None
        metered_usages = []  # To handle multiple meter readings

        # Process each item in the JSON data
        for item in invoice_json_data:
            key = item.get('Key')
            value = item.get('Value')

            # Check and process for total usage
            if 'Total Usage this period' in key:
                total_usage = value.split(' ')[0]  # Assuming usage is the first numeric value before a space

            # Check and process for metered usage
            elif 'Meter usage:' in key:
                usage_values = value.split(',')
                for usage in usage_values:
                    metered_usage = usage.split('=')[-1].strip().split(' ')[0]
                    metered_usages.append(metered_usage)

        # Validate and process the data
        if total_usage and metered_usages:
            usage_data = f"Found usage data: Total Usage = {total_usage} kWh, Metered Usages = {metered_usages}"
            model_input_texts = []
            for metered_usage in metered_usages:
                input_text = f"The total usage from the bill is {total_usage} kWh, compared to metered usage data received for the same billing period, which includes readings of {metered_usage} kWh."
                model_input_texts.append(input_text)
                # Optionally call invoke_bedrock_model here if needed
            return {'usage_data': usage_data, 'model_input_texts': model_input_texts}
        else:
            return {'error': "Missing usage data in JSON."}

    # Assume 'bucket_name' and 'json_object_key' are provided in the event
    bucket_name = 'bill-compare-hui'
    json_object_key = 'json-output/CI_Invoice_12107843_aaan.json'
        
    invoice_json_data = get_json_data_from_s3(bucket_name, json_object_key)
    if 'error' not in invoice_json_data:
        process_result = process_invoice_data(invoice_json_data)
        return {
            'statusCode': 200,
            'body': json.dumps(process_result)
        }
    else:
        return {
            'statusCode': 400,
            'body': json.dumps(invoice_json_data)  # Contains the error message
        }

