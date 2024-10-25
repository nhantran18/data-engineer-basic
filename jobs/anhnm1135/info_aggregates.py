import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lower, count, countDistinct
import pyspark.sql.functions as F
from joblib import variables as V
from pyspark.sql.utils import AnalysisException

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
# doc du lieu ngay gan nhat
data_path_now = 's3://ai4e-ap-southeast-1-dev-s3-data-landing/anhnm1135/data/silver/info_facts' # topic, keyword, author_cleansed, description_cleansed, cob_dt
destination_path = 's3://ai4e-ap-southeast-1-dev-s3-data-landing/anhnm1135/data/golden/info_aggregates'  # keyword, count_keyword, count_author, count_topic

#process 
df = spark.read.option('header', 'true').parquet(data_path_now)
filtered_df = df.filter(col("cob_dt") == date_string)
df_normalized = df.withColumn("keyword", lower(col("keyword")))

result_df = df_normalized.groupBy("keyword") \
    .agg(
        count("keyword").alias("new_count_keyword"),                        # Đếm số lần xuất hiện của keyword
        countDistinct("author_cleansed").alias("new_count_author"),         # Đếm số lượng tác giả khác nhau có từ khóa đó
        countDistinct("topic").alias("new_count_topic")                     # Đếm số lượng topic khác nhau có từ khóa đó
    )

## neu bang du lieu nguon da co 
try:
    df_des = spark.read.option('header', 'true').option('delimiter', ',').option('escape', '"').parquet(destination_path)
    df_keyword_merged = df_des.join(result_df, on = 'keyword', how = 'full_outer')
    df_keyword_updated = df_keyword_merged.withColumn(
        'count_keyword', 
        F.coalesce(F.col('count_keyword'), F.lit(0)) + F.coalesce(F.col('new_count_keyword'), F.lit(0))
    ).withColumn(
        'count_author', 
        F.coalesce(F.col('count_author'), F.lit(0)) + F.coalesce(F.col('new_count_author'), F.lit(0))
    ).withColumn(
        'count_topic', 
        F.coalesce(F.col('count_topic'), F.lit(0)) + F.coalesce(F.col('new_count_topic'), F.lit(0))
    ).withColumn(
        'cob_dt', F.lit(date_string)  # Đặt lại giá trị cob_dt
    )
    df_keyword_result = df_keyword_updated.select('keyword', 'count_keyword', 'count_author','count_topic', 'cob_dt')
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    df_keyword_result.write.mode('overwrite').partitionBy("cob_dt").parquet(destination_path)
#
except AnalysisException as e:
    print(f"Path does not exist or is inaccessible: {destination_path}")
    df_keyword_result = result_df.withColumnRenamed('new_count_keyword', 'count_keyword') \
                               .withColumnRenamed('new_count_author', 'count_author') \
                               .withColumnRenamed('new_count_topic', 'count_topic') \
                               .withColumn('cob_dt', F.lit(date_string))
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    df_keyword_result.write.mode('overwrite').partitionBy("cob_dt").parquet(destination_path)