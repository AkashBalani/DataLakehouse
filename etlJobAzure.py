import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Set Azure Storage account credentials
storage_account_name = os.getenv('STORAGE_NAME')
storage_account_key = os.getenv('STORAGE_KEY')
container_name = os.getenv('CONTAINER_NAME')
file_name = "adult_dataset.csv"

# Set configuration for accessing Azure Storage
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()
spark.conf.set("fs.azure.account.key."+storage_account_name +
               ".blob.core.windows.net", storage_account_key)

# Read the CSV file into a Spark DataFrame, remove all rows with missing values
file_path = "wasbs://"+container_name+"@" + \
    storage_account_name+".blob.core.windows.net/"+file_name
df = spark.read.csv(file_path, header=True, inferSchema=True)
df = df.filter(~col('Workclass').isin('?'))
df = df.filter(~col('Education').isin('?'))
df = df.filter(~col('Marital_Status').isin('?'))
df = df.filter(~col('Occupation').isin('?'))
df = df.filter(~col('Relationship').isin('?'))
df = df.filter(~col('Race').isin('?'))
df = df.filter(~col('Sex').isin('?'))
df = df.filter(~col('Native-Country').isin('?'))

df.show()

# unset configuration for remote file system creation after initialization (Hadoop 2.7 onwards).
spark.conf.unset("fs.azure.createRemoteFileSystemDuringInitialization")
# changing df into csv format and writing back to container
out_blob_path = "wasbs://" + container_name + "@" + \
    storage_account_name + ".blob.core.windows.net/" + "output_data.csv"
df.write.format("csv").mode("overwrite").option(
    "header", "true").save(out_blob_path)
