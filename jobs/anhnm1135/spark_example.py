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

### day la bang sau khi da update thong tin tung ngay
#### START JOB IMPLEMENTATION -> ETLs START
data_path = 's3://ai4e-ap-southeast-1-dev-s3-data-landing/anhnm1135/data_cafef.csv' # Bảng này thay đổi cập nhật mỗi ngày thì làm sao cập nhật path?
destination_path = 's3://ai4e-ap-southeast-1-dev-s3-data-landing/anhnm1135/anhnm_demo'

df = spark.read.option('header', 'true').option('delimiter', ',').option('escape', '"').csv(data_path)

df.printSchema()
df.show()

df.createOrReplaceTempView('footprint_view')

df_summary = spark.sql('''
SELECT COUNT(1) 
FROM footprint_view;
''')

df_summary.show(truncate=True)

df_summary.write.mode('overwrite').parquet(destination_path)

#### END JOB IMPLEMENTATION -> ETLs END
job.commit()
