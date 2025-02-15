import boto3
import json
from datetime import datetime
from io import StringIO
import pandas as pd
import csv
import logging


# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def put_object_to_s3(bucket, key, data_df):
    logger.info("Started uploading transformed file in s3 ...")
    try:
        # Convert DataFrame to CSV in-memory
        buffer = StringIO()
        data_df.to_csv(buffer, index=False, encoding='utf-8', quoting=csv.QUOTE_ALL)
        content = buffer.getvalue()

        # Upload CSV content to S3
        s3.put_object(Bucket=bucket, Key=key, Body=content)
        logger.info(f"File successfully uploaded to {bucket}/{key}")
        
        return key

    except Exception as e:
        logger.error(f"Error occurred while uploading file to {bucket}/{key}: {str(e)}")
        return None



def get_parsed_raw_jobs_data(json_raw_data):
    
    parsed_jobs = [] 
    
    for job in json_raw_data:
        parsed_jobs.append(
        dict(
            job_id = job['id'],
            job_title = job['title'],
            job_location = job['location']['display_name'],
            job_company = job['company']['display_name'],
            job_category = job['category']['label'],
            job_description = job['description'],
            job_url = job['redirect_url'],
            job_created = job['created']
        )
        )
    
    jobs_df = pd.DataFrame.from_dict(parsed_jobs)
    jobs_df['job_created'] = pd.to_datetime(jobs_df['job_created'])
    logger.info("Successfully extracted job postings data")
    return jobs_df
        
        
def delete_s3_object(bucket_name, object_key):
    try:
        # Delete the object
        s3.delete_object(Bucket=bucket_name, Key=object_key)
        logger.info(f"File {object_key} deleted successfully from bucket {bucket_name}.")

    except Exception as e:
        logger.error(f"Error occurred while deleting file: {str(e)}")


def move_s3_object(bucket, source_key, destination_key):
    logger.info(f"Started moving {source_key} to 'processed' folder in s3 ...")
    try:
        # Copy the object
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': source_key},
            Key=destination_key
        )

        logger.info(f"File copied from {source_key} to {destination_key} successfully.")
        delete_s3_object(bucket, source_key)

    except Exception as e:
        logger.error(f"Error occurred while copying file: {str(e)}")  
    

def lambda_handler(event, context):
    
    # Processing raw data
    logger.info("# Started processing raw data files")

    # Process the raw data file provided by the extract lambda function
    logger.info("Processing raw data file from previous Step Function output")

    s3_bucket = "crypto-etl-bucket"
    s3_object ="raw_data/to_processed/"
    
    objects = s3.list_objects(Bucket=s3_bucket, Prefix=s3_object)
    latest_object = max(objects['Contents'], key=lambda x: x['LastModified'])
    s3_object_key = latest_object['Key']
    
    print(s3_object_key)
    
    
    s3_object_data = s3.get_object(Bucket=s3_bucket, Key=s3_object_key)
    content = s3_object_data['Body']
    json_raw_data = json.loads(content.read())
    
   
            
    current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    jobs_transformed_data = get_parsed_raw_jobs_data(json_raw_data)
    print(jobs_transformed_data)
    s3_destination_key = f"transformed_data/to_migrate/adzuna_transformed_data_{current_timestamp}.csv"
    created_object_key = put_object_to_s3(s3_bucket, s3_destination_key, jobs_transformed_data)

    # Moving raw files from unprocessed to processed folder inside s3
    source_key = s3_object_key
    source_file_name = source_key.split('/')[-1]
    destination_key = "raw_data/processed/" + source_file_name
    move_s3_object(s3_bucket, source_key, destination_key)
    
    return created_object_key
    
    
