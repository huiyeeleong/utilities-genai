# import boto3
# import json

# def lambda_handler(event, context):
#     # Initialize the S3 client
#     s3 = boto3.client('s3')

#     # Check if 'body' is present in the event and parse it
#     if 'body' in event:
#         try:
#             event_body = json.loads(event['body'])
#             input_file_path = event_body.get('s3Path')
#         except json.JSONDecodeError as e:
#             return {
#                 'statusCode': 400,
#                 'body': json.dumps(f"Error parsing event body: {str(e)}")
#             }
#     else:
#         return {
#             'statusCode': 400,
#             'body': json.dumps("Error: Event does not contain a 'body'.")
#         }

#     if not input_file_path:
#         return {
#             'statusCode': 400,
#             'body': json.dumps("Error: 's3Path' not provided in the event body.")
#         }

#     # Specify the S3 bucket name
#     input_bucket_name = 'bill-compare-hui'
#     output_bucket_name = 'bill-compare-hui'

#     # Dynamically generate the output file path by replacing .json with .txt in the input file path
#     output_file_path = input_file_path.replace('.json', '.txt').replace('energy-output/', 'energy-output-organize/')


#     # Read the data from the input S3 bucket
#     try:
#         response = s3.get_object(Bucket=input_bucket_name, Key=input_file_path)
#         data = response['Body'].read().decode('utf-8').splitlines()
#     except Exception as e:
#         print(f"Error reading data from input S3: {str(e)}")
#         return {
#             'statusCode': 500,
#             'body': json.dumps(f"Error reading data from input S3: {str(e)}")
#         }

#     # Find the starting and ending indexes based on specific lines
#     start_index, end_index = None, None
#     for i, line in enumerate(data):
#         if "Your account in detail" in line:
#             start_index = i
#             break

#     for i, line in enumerate(data):
#         if "Meter type: Interval" in line:
#             end_index = i
#             break

#     # If both start and end indexes are found, extract the relevant section
#     if start_index is not None and end_index is not None:
#         account_detail_section = data[start_index:end_index + 1]
#         formatted_account_detail = "\n".join(account_detail_section)

#         # Upload the formatted account details to the output S3 bucket
#         try:
#             s3.put_object(Bucket=output_bucket_name, Key=output_file_path, Body=formatted_account_detail.encode('utf-8'))
#             return {
#                 'statusCode': 200,
#                 'body': json.dumps({
#                     "message": f"Formatted account details saved to S3: s3://{output_bucket_name}/{output_file_path}",
#                     "s3Bucket": output_bucket_name,
#                     "s3Key": output_file_path
#                 })
#             }
#         except Exception as e:
#             print(f"Error uploading the formatted account details to S3: {str(e)}")
#             return {
#                 'statusCode': 500,
#                 'body': json.dumps(f"Error uploading the formatted account details to S3: {str(e)}")
#             }
#     else:
#         print("Could not find the relevant account details section in the data.")
#         return {
#             'statusCode': 400,
#             'body': json.dumps("Could not find the relevant account details section in the data.")
#         }


import boto3
import json

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    try:
        event_body = json.loads(event.get('body', '{}'))  # More resilient handling
        input_file_path = event_body.get('s3Path')
    except json.JSONDecodeError as e:
        return {'statusCode': 400, 'body': json.dumps(f"Error parsing event body: {str(e)}")}

    if not input_file_path:
        return {'statusCode': 400, 'body': json.dumps("Error: 's3Path' not provided in the event body.")}

    input_bucket_name = 'bill-compare-hui'
    output_bucket_name = 'bill-compare-hui'
    output_file_path = input_file_path.replace('.json', '_account_details.txt').replace('energy-output/', 'energy-output-organize/')

    try:
        response = s3.get_object(Bucket=input_bucket_name, Key=input_file_path)
        data = response['Body'].read().decode('utf-8').splitlines()
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps(f"Error reading data from S3: {str(e)}")}

    account_details = extract_account_details(data)

    if account_details:
        try:
            s3.put_object(Bucket=output_bucket_name, Key=output_file_path, Body="\n".join(account_details).encode('utf-8'))
            return {
                'statusCode': 200,
                'body': json.dumps({
                    "message": f"Account details saved to S3: s3://{output_bucket_name}/{output_file_path}",
                    "s3Bucket": output_bucket_name,
                    "s3Key": output_file_path
                })
            }
        except Exception as e:
            return {'statusCode': 500, 'body': json.dumps(f"Error uploading account details to S3: {str(e)}")}
    else:
        return {'statusCode': 400, 'body': json.dumps("Could not find account details in the document.")}

def extract_account_details(data_lines):
    account_details = []
    capture = False
    for line in data_lines:
        if "Account Number" in line or "Account Nbr" in line:
            capture = True
        if capture:
            account_details.append(line)

    return account_details
