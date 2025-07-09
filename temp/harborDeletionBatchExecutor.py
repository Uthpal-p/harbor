import os
import csv
import io
import json
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed

s3_client = boto3.client('s3')


def delete_batch(s3_client, target_s3_bucket, batch):
    """
    Helper function to delete a single batch of S3 objects.
    This function is designed to be run in a separate thread.

    Args:
        s3_client (boto3.client): Initialized S3 client.
        target_s3_bucket (str): Name of the S3 bucket from which objects will be deleted.
        batch (list): A list of dictionaries, each with a 'Key' representing an S3 object.

    Returns:
        tuple: A tuple containing two lists:
               - List of successfully deleted object keys.
               - List of dictionaries for failed objects (Key, Code, Message).
    """
    batch_deleted_objects_count = 0
    batch_failed_objects = []

    if not batch:
        return batch_deleted_objects_count, batch_failed_objects

    try:
        response = s3_client.delete_objects(
            Bucket=target_s3_bucket,
            Delete={'Objects': batch, 'Quiet': False}
        )

        if 'Deleted' in response:
            batch_deleted_objects_count = len(response['Deleted'])

        if 'Errors' in response:
            for error_item in response['Errors']:
                batch_failed_objects.append({
                    'Key': error_item['Key'],
                    'Code': error_item['Code'],
                    'Message': error_item['Message']
                })
                print(
                    f"WARNING: Failed to delete {error_item['Key']}. Code: {error_item['Code']}, Message: {error_item['Message']}")

    except s3_client.exceptions.NoSuchBucket as e:
        print(
            f"CRITICAL ERROR in batch deletion: Target S3 bucket '{target_s3_bucket}' does not exist. {e}")
        for obj in batch:
            batch_failed_objects.append({
                'Key': obj['Key'],
                'Code': 'NoSuchBucket',
                'Message': f"Target bucket '{target_s3_bucket}' not found: {e}"
            })
    except Exception as e:
        print(
            f"ERROR: An unexpected error occurred during batch deletion API call: {e}")
        for obj in batch:
            batch_failed_objects.append({
                'Key': obj['Key'],
                'Code': 'API_CALL_ERROR',
                'Message': str(e)
            })

    return batch_deleted_objects_count, batch_failed_objects


def lambda_handler(event, context):
    """
    Lambda function that reads a specific range of lines from a CSV file
    in S3 and processes them (e.g., for deletion).

    Expected event payload:
    {
        "bucket_name": "my-csv-data-bucket",
        "csv_key": "your_file.csv",
        "start_line": 0,    # 0-indexed start line (inclusive)
        "end_line": 9999    # 0-indexed end line (inclusive)
    }
    """
    bucket_name = event.get('bucket_name')
    csv_key = event.get('csv_key')
    start_line = event.get('start_line')
    end_line = event.get('end_line')
    target_s3_bucket = bucket_name

    if not all([bucket_name, csv_key, start_line is not None, end_line is not None]):
        print("Error: Missing required parameters in event payload.")
        return {
            'statusCode': 400,
            'body': json.dumps('Missing required parameters (bucket_name, csv_key, start_line, end_line).')
        }

    print(f"Deletion Lambda processing: s3://{bucket_name}/{csv_key}")
    print(f"Lines to process: {start_line} to {end_line}")

    processed_count = 0

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=csv_key)
        csv_content = response['Body'].read().decode('utf-8')
        csv_file = io.StringIO(csv_content)

        # Using csv.reader for raw line access to easily skip to start_line
        # If your CSV has a header, account for it. Assuming first line is header.
        reader = csv.reader(csv_file)
        header = next(reader)  # Read header

        line_num = 0  # This counter tracks the data lines (after header)
        object_keys_to_delete = []
        failed_objects = []
        total_objects_processed_by_api = 0
        deleted_objects_count = 0

        for row_data in reader:
            if start_line <= line_num <= end_line:
                if row_data and row_data[1].strip():
                    object_keys_to_delete.append(
                        {'Key': row_data[1].strip()})

                processed_count += 1
            elif line_num > end_line:
                # If we've passed our range, no need to read further
                break
            line_num += 1

        total_objects_in_batch = len(object_keys_to_delete)
        print(
            f"Found {total_objects_in_batch} objects to potentially delete from the batch.")

        # S3 delete_objects API supports up to batch_size objects per call.
        batch_size = 1000
        max_workers = min(
            5, (total_objects_in_batch + batch_size - 1) // batch_size or 1)
        print(f"Using {max_workers} threads for parallel deletion.")

        # 3. Parallelize batch deletion using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for i in range(0, total_objects_in_batch, batch_size):
                batch = object_keys_to_delete[i: i + batch_size]
                if not batch:
                    continue
                total_objects_processed_by_api += len(batch)
                print(
                    f"Submitting batch {int(i/batch_size) + 1} with {len(batch)} objects for deletion.")
                futures.append(executor.submit(
                    delete_batch, s3_client, target_s3_bucket, batch))

            # 4. Collect results from completed futures
            for future in as_completed(futures):
                try:
                    batch_deleted_count, batch_failed = future.result()
                    deleted_objects_count = deleted_objects_count + batch_deleted_count
                    failed_objects.extend(batch_failed)
                except Exception as e:
                    print(
                        f"ERROR: An error occurred retrieving results from a batch deletion future: {e}")

        print(
            f"Deletion Lambda finished. Processed {processed_count} lines within the range.")

        # Final Report Generation
        report = {
            'total_objects_in_csv': (end_line-start_line+1),
            'total_objects_attempted_for_deletion': total_objects_processed_by_api,
            'successfully_deleted_count': deleted_objects_count,
            'failed_to_delete_count': len(failed_objects),
            'failed_objects_list': failed_objects
        }

        print("\n--- S3 Deletion Summary ---")
        print(f"Total objects listed in CSV: {report['total_objects_in_csv']}")
        print(
            f"Total objects for which deletion was attempted: {report['total_objects_attempted_for_deletion']}")
        print(
            f"Successfully deleted: {report['successfully_deleted_count']} objects")
        if (len(failed_objects)):
            print(f"Failed to delete: {len(failed_objects)} objects")
        if report['failed_objects_list']:
            for failure in report['failed_objects_list']:
                print(
                    f"  - Key: {failure['Key']}, Error Code: {failure['Code']}, Message: {failure['Message']}")
        print("----------------------------")
        if (len(report['failed_objects_list']) == 0):
            return {
                'statusCode': 200,
                'body': json.dumps(report)
            }
        else:
            return {
                'statusCode': 500,
                'body': json.dumps(report)
            }

    except Exception as e:
        print(
            f"Error in Deletion Lambda for {csv_key} (lines {start_line}-{end_line}): {e}")
        raise ValueError(
            f"Error in Deletion Lambda for {csv_key} (lines {start_line}-{end_line})")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing batch: {e}')
        }
