import os
from pyspark.sql import SparkSession
import boto3

aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

# Set up Spark Session
spark = SparkSession.builder \
    .appName("ETLJob") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
    .getOrCreate()

# Step 0: Create S3 Buckets (if not exists)
s3_client = boto3.client('s3')


landing_bucket = 'landing-bucket-akashbalani'
trusted_bucket = 'trusted-bucket-akashbalani'
curated_bucket = 'curated-bucket-akashbalani'

# Create Landing Bucket
s3_client.create_bucket(Bucket=landing_bucket)

# Create Trusted Bucket
s3_client.create_bucket(Bucket=trusted_bucket)

# Create Curated Bucket
s3_client.create_bucket(Bucket=curated_bucket)

# Step 1: Upload Data to Landing Zone
landing_zone_path = f's3a://{landing_bucket}/adult_dataset.csv'
# Replace with the local path to your downloaded Adult dataset
local_dataset_path = 'adult_dataset.csv'
s3_client.upload_file(local_dataset_path, landing_bucket, 'adult_dataset.csv')

# Step 2: Extract Data from Landing Zone
df = spark.read.csv(landing_zone_path, header=True, inferSchema=True)

# Step 3: Transform Data (Sanitize using Spark)
# Example: Remove duplicates
df = df.dropDuplicates()

df = df.coalesce(1)
# Step 4: Load Data to Trusted Zone
trusted_zone_path = f's3a://{trusted_bucket}/adult_trusted.csv'
df.write.csv(trusted_zone_path, mode='overwrite', header=True)

# Step 5: Further transformations as needed for curated data
# Example: Select specific columns
curated_df = df.select("age", "workclass", "education",
                       "marital_status", "occupation", "income")

curated_df = curated_df.coalesce(1)
# Step 6: Load Curated Data to Curated Zone
curated_zone_path = f's3a://{curated_bucket}/adult_curated.csv'
curated_df.write.csv(curated_zone_path, mode='overwrite', header=True)
