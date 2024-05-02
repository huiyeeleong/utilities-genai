import boto3
import json

# Initialize clients outside the handler to take advantage of connection reuse
dynamodb_client = boto3.client('dynamodb')
bedrock_client = boto3.client('bedrock-runtime')

def lambda_handler(event, context):
    def string_to_float(value):
        # Remove commas used as thousands separators
        return float(value.replace(',', ''))

    def get_year_from_invoice(invoice_data):
        # Extracts year from the 'Date_of_Issue' field in invoice data
        date_of_issue = invoice_data.get('Date_of_Issue', {}).get('S', '')
        return date_of_issue.split('/')[-1]  # Assuming the date format is DD/MM/YYYY

    def get_invoice_data(table_name, file_name_key):
        # Retrieve the invoice data from DynamoDB
        response = dynamodb_client.get_item(
            TableName=table_name,
            Key={'File_Name': {'S': file_name_key}}
        )
        return response['Item']

    def calculate_variation(current, previous):
        if previous == 0:
            return float('inf')  # Avoid division by zero; return infinity
        return ((current - previous) / previous) * 100.0

    def invoke_bedrock_model(input_text):
        # Construct the Bedrock API request payload
        payload = {
            "modelId": "amazon.titan-embed-text-v1",
            "contentType": "application/json",
            "accept": "*/*",
            "body": {
                "inputText": input_text
            }
        }
        
        # Use the AWS SDK to make the Bedrock API call
        response = bedrock_client.invoke_model(
            body=json.dumps(payload['body']),
            modelId=payload['modelId'],
            accept=payload['accept'],
            contentType=payload['contentType'],
        )
        
        # Read the response content from the StreamingBody
        response_content = response['body'].read().decode('utf-8')  # Correct way to read from StreamingBody
        return json.loads(response_content)

    # Assume event contains the necessary keys
    table_name = 'hui-billing-data'  # Replace with your actual table name
    new_invoice_file_name = '0f1_arstmto3_14303_21_E_1'  # Replace with your actual new invoice file name
    last_year_invoice_file_name = '0f1_arstmto3_20307_15_E_1'  # Replace with your actual last year's invoice file name
    
    new_invoice_data = get_invoice_data(table_name, new_invoice_file_name)
    last_invoice_data = get_invoice_data(table_name, last_year_invoice_file_name)
    
    new_invoice_year = get_year_from_invoice(new_invoice_data)
    last_invoice_year = get_year_from_invoice(last_invoice_data)

    # Convert the retrieved data from DynamoDB's format
    new_total_cost = string_to_float(new_invoice_data['Total_Due']['S'])
    last_total_cost = string_to_float(last_invoice_data['Total_Due']['S'])
    new_bill_amount = string_to_float(new_invoice_data['Bill_Amount']['S'])
    last_bill_amount = string_to_float(last_invoice_data['Bill_Amount']['S'])
    
    # Calculate variations
    cost_variation = calculate_variation(new_total_cost, last_total_cost)
    bill_amount_variation = calculate_variation(new_bill_amount, last_bill_amount)
    
    # Construct the input text for the Bedrock AI model
    input_text = f"New invoice total cost is {new_total_cost}, last year's was {last_total_cost}. " \
                 f"New invoice bill amount is {new_bill_amount}, last year's was {last_bill_amount}. " \
                 f"Cost variation is {cost_variation}%, bill amount variation is {bill_amount_variation}%."
    
    # Invoke the Bedrock model and get the response
    bedrock_response = invoke_bedrock_model(input_text)
    
    response = {
        'cost_variation': cost_variation,
        'bill_amount_variation': bill_amount_variation,
        'messages': []
    }
    
    # Check and print if there are excessive variations
    if cost_variation > 15:
        message = f"Excessive Spend Variation detected for invoice {new_invoice_file_name} ({new_invoice_year}): {cost_variation:.2f}% increase compared to {last_invoice_year}."
        response['messages'].append(message)
    else:
        message = f"Spend variation for invoice {new_invoice_file_name} ({new_invoice_year}) is within acceptable limits: {cost_variation:.2f}% increase compared to {last_invoice_year}."
        response['messages'].append(message)
    
    if bill_amount_variation > 15:
        message = f"Excessive Bill Amount (Usage) Variation detected for invoice {new_invoice_file_name} ({new_invoice_year}): {bill_amount_variation:.2f}% increase compared to {last_invoice_year}."
        response['messages'].append(message)
    else:
        message = f"Bill amount (Usage) variation for invoice {new_invoice_file_name} ({new_invoice_year}) is within acceptable limits: {bill_amount_variation:.2f}% increase compared to {last_invoice_year}."
        response['messages'].append(message)
    
    # Instead of printing, return the response object
    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }
