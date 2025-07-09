import os
import csv
import io
import json
import math
import boto3

s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

# Configuration
# Name of the Deletion Lambda function to invoke
DELETION_LAMBDA_FUNCTION_NAME = os.environ.get(
    'DELETION_LAMBDA_FUNCTION_NAME', 'HarborDeletionBatchExecutor')  # Default name
BATCH_SIZE = 10000  # Number of lines per batch for the Deletion Lambda


def lambda_handler(event, context):
    """
    Lambda function that orchestrates CSV file processing.
    It reads the CSV, calculates batches, and invokes the Deletion Lambda
    for each batch.

    Expected event payload (for direct invocation, or can be S3 trigger):
    {
        "bucket_name": "my-csv-data-bucket",
        "csv_key": "your_file.csv"
    }
    """
    bucket_name = None
    csv_file_key = None

    # Determine how the Lambda was invoked (S3 event or direct invocation)
    if 'Records' in event and len(event['Records']) > 0 and 's3' in event['Records'][0]:
        # S3 event trigger
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        csv_file_key = record['s3']['object']['key']
    elif 'csv_bucket_name' in event and 'csv_key' in event:
        # Direct invocation with custom payload
        bucket_name = event['csv_bucket_name']
        csv_file_key = event['csv_key']
    else:
        print("Error: Invalid event structure. Unable to determine bucket and key.")
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid event structure. Provide bucket_name and csv_key or use S3 trigger.')
        }

    if not DELETION_LAMBDA_FUNCTION_NAME:
        print("Error: DELETION_LAMBDA_FUNCTION_NAME environment variable is not set.")
        return {
            'statusCode': 500,
            'body': json.dumps('DELETION_LAMBDA_FUNCTION_NAME not configured.')
        }

    print(f"Orchestrator processing CSV: s3://{bucket_name}/{csv_file_key}")

    try:
        # Step 1: Read the CSV to count the total number of lines
        response = s3_client.get_object(Bucket=bucket_name, Key=csv_file_key)
        csv_content = response['Body'].read().decode('utf-8')
        csv_file = io.StringIO(csv_content)

        # Count data rows (skip header if present)
        # We use csv.reader here to count lines without DictReader overhead
        # Assuming the first line is a header
        raw_reader = csv.reader(csv_file)
        # next(raw_reader, None)  # no header row

        total_data_rows = sum(1 for row in raw_reader)
        print(f"Total data rows in CSV: {total_data_rows}")

        # Step 2: Calculate the number of batches
        num_batches = math.ceil(total_data_rows / BATCH_SIZE)
        print(f"Calculated {num_batches} batches with size {BATCH_SIZE}.")

        invocations_successful = 0
        invocations_failed = 0

        # Step 3: Invoke Deletion Lambda for each batch
        for i in range(num_batches):
            start_line_index = i * BATCH_SIZE
            # end_line_index is inclusive
            end_line_index = min((i + 1) * BATCH_SIZE - 1, total_data_rows - 1)

            payload = {
                "bucket_name": bucket_name,
                "csv_key": csv_file_key,
                "start_line": start_line_index,
                "end_line": end_line_index
            }

            print(
                f"Invoking {DELETION_LAMBDA_FUNCTION_NAME} for batch {i+1}/{num_batches} (lines {start_line_index}-{end_line_index}).")

            try:
                # Invoke the Deletion Lambda asynchronously
                response = lambda_client.invoke(
                    FunctionName=DELETION_LAMBDA_FUNCTION_NAME,
                    InvocationType='Event',  # Asynchronous invocation
                    Payload=json.dumps(payload)
                )
                if response['StatusCode'] < 200 or response['StatusCode'] >= 300:
                    print(
                        f"Warning: Deletion Lambda invocation for batch {i} returned status {response['StatusCode']}")
                    invocations_failed += 1
                else:
                    if response['StatusCode'] == 202:
                        print(
                            f"Deletion Lambda invocation for batch {i} accepted with status 202.")
                    invocations_successful += 1
            except Exception as invoke_e:
                print(
                    f"Error invoking Deletion Lambda for batch {i}: {invoke_e}")
                invocations_failed += 1

        print(
            f"Orchestrator finished. Total batches: {num_batches}, Successful invocations: {invocations_successful}, Failed invocations: {invocations_failed}.")

        if invocations_failed > 0:
            return {
                'statusCode': 202,  # Accepted but with some failures
                'body': json.dumps(f'Orchestration completed with {invocations_successful} successful and {invocations_failed} failed batch invocations.')
            }
        else:
            return {
                'statusCode': 200,
                'body': json.dumps('Orchestration completed successfully, all deletion batches triggered.')
            }

    except Exception as e:
        print(f"Error in Orchestrator Lambda for {csv_file_key}: {e}")
        raise ValueError("Something went wrong during orchestration!")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Orchestrator failed: {e}')
        }
