import boto3
import json

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    # Assuming input parameters are provided through the event object
    # # If not, you'll need to adjust this to retrieve parameters as needed
    # input_bucket_name = 'bill-compare-hui'
    # input_file_path = 'energy-output-organize/CI_Invoice_12107843_aaan.txt'
    # output_bucket_name = 'bill-compare-hui'
    # output_file_path = input_file_path.replace('energy-output-organize', 'energy-output-organized-results').replace('.txt', '_organized.txt')
# Extract 's3Path' from the event, if available
    try:
        event_body = json.loads(event.get('body', '{}'))
        input_file_path = event_body.get('s3Key')
    except json.JSONDecodeError as e:
        return {'statusCode': 400, 'body': json.dumps(f"Error parsing event body: {str(e)}")}
    
    if not input_file_path:
        return {'statusCode': 400, 'body': json.dumps("Error: 's3Key' not provided in the event body.")}
    
    input_bucket_name = 'bill-compare-hui'
    output_bucket_name = 'bill-compare-hui'
    
    # Dynamically generate the output file path
    output_file_path = input_file_path.replace('energy-output-organize', 'energy-output-organized-results').replace('.txt', '_organized.txt')    
    try:
        response = s3.get_object(Bucket=input_bucket_name, Key=input_file_path)
        data = response['Body'].read().decode('utf-8').splitlines()
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps(f"Error reading data from input S3: {str(e)}")}
    
    organized_data = organize_data(data)
    formatted_data = format_organized_data_for_s3(organized_data)
    
    try:
        s3.put_object(Bucket=output_bucket_name, Key=output_file_path, Body=formatted_data.encode('utf-8'))
        # Return the S3 key of the uploaded file for the next job to use
        return {'statusCode': 200, 'body': json.dumps({'s3Key': output_file_path})}
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps(f"Error uploading the organized data to S3: {str(e)}")}

def organize_data(data):
    organized_data = {}
    current_section = None
    
    for line in data:
    # Check if the line represents a section header and reset section details capture
        if any(header in line for header in ["Energy Charges", "Network Charges", "Renewable Energy Charges", "Other Charges"]):
            current_section = line.strip()
            organized_data[current_section] = []
        elif "Meter Information" in line:  # Handling "Meter Information" as a special case
            current_section = "Meter Information"
            organized_data[current_section] = []
        elif current_section:
            if ":" in line:
                line = line.replace(':', '')  # Remove colon for consistency
                details = line.strip().split(" ", 1)
                if len(details) >= 2:
                    organized_data[current_section].append(details)
            else:
                organized_data[current_section].append([line.strip()])

    return organized_data


def format_organized_data_for_s3(data):
    formatted_data = ""
    for section, details in data.items():
        formatted_data += section + "\n"
        for detail in details:
            if isinstance(detail, list):
                formatted_data += f"{detail[0]}: {' '.join(detail[1:])}\n"
            else:
                formatted_data += detail + "\n"
        formatted_data += "\n"  # Add an extra newline for spacing between sections
    return formatted_data
