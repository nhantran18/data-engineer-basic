import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from joblib import variables as V
from pyspark.sql import functions as F

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print(args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from raw file then implement SCD CDC as designed
# Read Fact table and then add column date from transaction time
# Write to parquet table
# Run create athena query
#### START JOB FACT IMPLEMENTATION -> ETLs START
# Declare location of raw and write destination after processing raw file
fact_data_path = 's3://ai4e-ap-southeast-1-dev-s3-data-landing/nhung268/raw/payment_transactions/payment_transactions_20240930.csv' # Cloud path
destination_path = 's3://ai4e-ap-southeast-1-dev-s3-data-landing/nhung268/standardize/payment_transactions'

# read data from csv raw file
df_payment = spark.read.option('header', 'true').option('delimiter', ',').option('escape', '"').csv(fact_data_path)

# Aadd column:
df_payment_fact = df_payment.withColumn('data_date', F.to_date(F.to_timestamp(df_payment.transaction_time, 'yyyy-MM-dd HH:mm:ss.SSS'), 'yyyy-MM-dd'))
df_payment_fact.show(truncate=False)
df_payment_fact.write.mode('overwrite').partitionBy('data_date').parquet(destination_path)
#### END JOB FACT IMPLEMENTATION -> ETLs END

#### START JOB DIMENTION IMPLEMENTATION -> ETLs START
# Declare location of raw and write destination after processing raw file
dimention_data_path = 's3://ai4e-ap-southeast-1-dev-s3-data-landing/nhung268/raw/account/account_20240930.csv' # Cloud path
dim_destination_path = 's3://ai4e-ap-southeast-1-dev-s3-data-landing/nhung268/standardize/account'

# read data from csv raw file
df_account = spark.read.option('header', 'true').option('delimiter', ',').option('escape', '"').csv(dimention_data_path)

# Aadd column:
df_account_dim = df_account.withColumn('data_date', args['data_date'])
df_account_dim.show(truncate=False)
df_account_dim.write.mode('overwrite').partitionBy('data_date').parquet(dim_destination_path)
#### END JOB DIMENTION IMPLEMENTATION -> ETLs END


# Cần implementation Spark code
# Cần resource aws -> aws resource management


job.commit()
