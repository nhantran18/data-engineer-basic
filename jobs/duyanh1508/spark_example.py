import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from joblib import variables as V

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print(args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#### START JOB IMPLEMENTATION -> ETLs START
data_path = 's3://ai4e-ap-southeast-1-dev-s3-data-landing/data_test/02_interactions.csv' # Cloud path
destination_path = 's3://ai4e-ap-southeast-1-dev-s3-data-landing/data_test/duyanh1508demo_data'

df = spark.read.option('header', 'true').option('delimiter', ',').option('escape', '"').csv(data_path)

df.printSchema()
df.show()

df.createOrReplaceTempView('interactions')

df_summary = spark.sql('''
select distinct username, email 
from interaction
''')

df_summary.show(truncate=True)

df_summary.write.mode('overwrite').parquet(destination_path)

#### END JOB IMPLEMENTATION -> ETLs END
job.commit()
