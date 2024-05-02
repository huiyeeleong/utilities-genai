import boto3
import json
import os

def lambda_handler(event, context):
    # Initialize S3 client
    s3_client = boto3.client('s3')

    # Extract bucket name and JSON object key from the event
    
    bucket_name = 'bill-compare-hui'
    object_key = event.get('s3Key')  # The S3 key is expected to be passed in the event
    
    json_file_name = os.path.splitext(os.path.basename(object_key))[0] + '.json'
    json_object_key = 'json-output/' + json_file_name  # Constructs the S3 path for the JSON output

    # Fetch the JSON file from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=json_object_key)
    json_file_content = response['Body'].read().decode('utf-8')

    # Parse the JSON file
    data = json.loads(json_file_content)
    
    # Specify your column names here
    column_names = [
    "Company_Name", "Debtor_ID", "Account_Number", "Invoice_Number", "Date_of_Issue",
    "ABN", "Telephone", "Email", "Payment_Due", "Account_Name", "Address", "Opening_Balance",
    "Bill_Amount", "Total_Due", "Last_Bill_Amount", "Payments", "Energy_Supply_Address", "NMI",
    "Next_Read_Date", "Meter_Number", "Supply_Start_Date", "Supply_End_Date", "OnPeak_Units",
    "OffPeak_Units", "OnPeak_Energy_Charge", "OffPeak_Energy_Charge", "Supply_Charge", "GST",
    "Total", "Document_Set_ID", "Version", "Version_Date", "Comments", "Status", "Created_Date",
    "Created_Time"
    ]
    
    json_string = json.dumps(data, indent=2)

    prompt_data = {
        "prompt": f"Thoroughly examine the following JSON data and match each available piece of information to these columns:\n\n{', '.join(column_names)}\n\nJSON Data:\n\n{json_string}\n\nEnsure that every piece of data in the JSON file is accounted for. Map each value to the corresponding column name, and only leave blanks for data that is not present in the JSON file. Provide the results in a key-value pair json format.",
        "maxTokens": 8191,
        "temperature": 0.8,
        "topP": 0.8,
    }

    # Assuming a custom integration for invoking the AI model
    # This section needs adjustment based on the actual service details and API
    try:
    # Invoke Bedrock AI Model
        bedrock = boto3.client(service_name="bedrock-runtime")
        response = bedrock.invoke_model(
            body=json.dumps(prompt_data),
            modelId="ai21.j2-ultra-v1",
            accept="application/json",
            contentType="application/json",
        )
    
        response_body = json.loads(response['body'].read().decode('utf-8'))
        response_text = response_body.get("completions")[0].get("data").get("text")
    
        # Output the AI model's response
        print("AI Model's Response:")
        print(response_text)
    
        # Save the response in a new folder on S3
        new_folder = 'ai-model-output'
        output_file_name = json_object_key.split('/')[-1]  # Use the same file name
        new_object_key = f"{new_folder}/{output_file_name}"
    
        s3_client.put_object(Bucket=bucket_name, Key=new_object_key, Body=response_text.encode('utf-8'))
        print(f"AI's response saved to s3://{bucket_name}/{new_object_key}")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "AI model invoked and output saved successfully.",
                "s3Path": f"{bucket_name}/{new_object_key}"
            })
        }
    
    except Exception as e:
        print(f"Error during process: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": "Failed to process due to an error.",
                "errorMessage": str(e)
            })
        }
