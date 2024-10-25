import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, sum, explode, split, col, regexp_replace, lit
import pyspark.sql.functions as F
from joblib import variables as V

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print(args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

### day la bang sau khi da update thong tin tung ngay with format yyyy-mm-dd
run_data_date = args.get('data_date')    
current_time = datetime.now()
# Tiến hành lấy ngày chạy job để update vào bảng dữ liệu
# Nếu không có ngày chạy job thì lấy ngày hiện tại
date_string = run_data_date if run_data_date else f"{str(current_time.year)}-{str(current_time.month).zfill(2)}-{str(current_time.day).zfill(2)}"

#### START JOB IMPLEMENTATION -> ETLs START
data_path_now = 's3://ai4e-ap-southeast-1-dev-s3-data-landing/anhnm1135/data/raw/cafef_{}.csv'.format(date_string) # Bảng này thay đổi cập nhật mỗi ngày thì làm sao cập nhật path?
destination_path = 's3://ai4e-ap-southeast-1-dev-s3-data-landing/anhnm1135/silver/keywords_summary'

df = spark.read.option('header', 'true').option('delimiter', ',').option('escape', '"').csv(data_path_now)
df_cleaned = df.withColumn("keywords", F.trim(regexp_replace(col("keywords"), r"[\[\]']", "")))

# Tách các từ khóa bằng cách sử dụng explode và split
df_split = df_cleaned.withColumn("keyword", explode(split(col("keywords"), ", ")))

df_keyword_count = df_split.groupBy("keyword").count()
keyword_dict = {row['keyword']: row['count'] for row in df_keyword_count.collect()}

data = [Row(keyword=key, count=value) for key, value in keyword_dict.items()]

df = spark.createDataFrame(data)

df_with_cob_dt = df.withColumn("cob_dt", lit(date_string))

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
df_with_cob_dt.write.mode('overwrite').partitionBy("cob_dt").parquet(destination_path)

#### END JOB IMPLEMENTATION -> ETLs END
job.commit()
