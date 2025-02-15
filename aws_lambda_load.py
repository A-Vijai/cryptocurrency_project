import boto3
import os
import logging
from botocore.exceptions import ClientError


# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Redshift creds
REDSHIFT_REGION = 'us-east-1'  # Change to the region your Redshift Serverless is in
REDSHIFT_WORKGROUP = 'crypto-etl-project'  
REDSHIFT_SECRET_ARN = os.getenv('REDSHIFT_SECRET_ARN')
REDSHIFT_IAM_ROLE = os.getenv('REDSHIFT_IAM_ROLE')

# Initialize the Secrets Manager client
session = boto3.session.Session()

# Initialize the Redshift Data API client
client_redshift = session.client('redshift-data', region_name=REDSHIFT_REGION)


QUERY_TEMPLATE = """
BEGIN;

TRUNCATE TABLE adzuna.stg.staging_jobs;

COPY adzuna.stg.staging_jobs 
FROM '{s3_uri}' 
IAM_ROLE '{iam_role}' 
FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 
REGION AS '{region}';

INSERT INTO adzuna.dw.jobs
SELECT stj.*    
FROM adzuna.stg.staging_jobs stj
LEFT JOIN adzuna.dw.jobs dwj ON stj.job_id = dwj.job_id
WHERE dwj.job_id IS NULL;

COMMIT;
"""

def execute_redshift_query(query_str, database='adzuna'):
    try:
        logger.info(f"Executing SQL query: {query_str}")
        response = client_redshift.execute_statement(
            Database=database,  # Redshift database name
            SecretArn=REDSHIFT_SECRET_ARN,
            Sql=query_str,
            WorkgroupName=REDSHIFT_WORKGROUP 
         )
        logger.info("Query executed successfully")
        logger.info(f"Response: {response}")
        
    except ClientError as e:
        logger.error(f"Error executing query: {e}")
        raise e
    
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        raise e


def lambda_handler(event,context):
    
    client = boto3.client('s3')
    
    s3_bucket = 'crypto-etl-bucket'
    s3_object = 'transformed_data/to_migrate/'
    objects = client.list_objects(Bucket=s3_bucket, Prefix=s3_object)
    latest_object = max(objects['Contents'], key=lambda x: x['LastModified'])
    s3_object_key = latest_object['Key']
    
    
    s3_uri = f's3://{s3_bucket}/{s3_object_key}'
    print(s3_uri)
    
    logger.info(f"S3 URI of transformed file: {s3_uri}")
    
    query = QUERY_TEMPLATE.format(s3_uri=s3_uri, iam_role=REDSHIFT_IAM_ROLE, region=REDSHIFT_REGION)
    logger.info(f"Redshift query to execute: {query}")
    
    
    logger.info("Started importing data from csv file in s3 and merging to dw table")
    execute_redshift_query(query_str=query)
    
    return s3_object_key
    