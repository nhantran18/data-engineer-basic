import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from datetime import datetime
from joblib import variables as V
from pyspark.sql import functions as F

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME']) # Lấy tham số từ Glue Job
print(args)
# Khởi tạo Glue context
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

# Đọc dữ liệu từ S3
fact_data_path = f"s3://ai4e-ap-southeast-1-dev-s3-data-landing/tructt321/Rawdata_Cryptocurrency{datetime.now().strftime('%Y-%m-%d')}*" # Cloud path
# read data from csv raw file
df_rawdata = spark.read.option('header', 'true').option('delimiter', ',').option('escape', '"').csv(fact_data_path)



# Hiển thị file rawdata
df_rawdata.show()


# Aadd column:

#### END JOB FACT IMPLEMENTATION -> ETLs END

#### START JOB DIMENTION IMPLEMENTATION -> ETLs START
# Declare location of raw and write destination after processing raw file

# Tạo bảng mới chứa thông tin về biến động giá tiền
df3 = df_rawdata.groupBy("name").agg(
    F.mean("`quote.USD.percent_change_1h`").alias("percent_change_1h"),
    F.mean("`quote.USD.percent_change_24h`").alias("percent_change_24h"),
    F.mean("`quote.USD.percent_change_7d`").alias("percent_change_7d"),
    F.mean("`quote.USD.percent_change_30d`").alias("percent_change_30d"),
    F.mean("`quote.USD.percent_change_60d`").alias("percent_change_60d"),
    F.mean("`quote.USD.percent_change_90d`").alias("percent_change_90d")
)

# Chuyển đổi từ dạng wide sang dạng long
df4 = df3.selectExpr("name", 
    "stack(6, 'percent_change_1h', percent_change_1h, 'percent_change_24h', percent_change_24h, \
               'percent_change_7d', percent_change_7d, 'percent_change_30d', percent_change_30d, \
               'percent_change_60d', percent_change_60d, 'percent_change_90d', percent_change_90d) \
     as (percent_change, values)"
)

# Đổi tên các giá trị của cột 'percent_change'
df5 = df4.withColumn(
    "percent_change", 
    F.when(F.col("percent_change") == "percent_change_1h", "1h")
     .when(F.col("percent_change") == "percent_change_24h", "24h")
     .when(F.col("percent_change") == "percent_change_7d", "7d")
     .when(F.col("percent_change") == "percent_change_30d", "30d")
     .when(F.col("percent_change") == "percent_change_60d", "60d")
     .when(F.col("percent_change") == "percent_change_90d", "90d")
)

################### Thông tin lưu file
# Tạo một đường dẫn mới với timestamp (ngày giờ hiện tại)
destination_path = f"s3://ai4e-ap-southeast-1-dev-s3-data-landing/tructt321/percent_change/percent_change{datetime.now().strftime('%Y-%m-%d')}.parquet"

# Lưu DataFrame vào file Parquet mới
df5.write.mode('overwrite').parquet(destination_path)

print(f"Data saved to {destination_path}")

#### END JOB DIMENTION IMPLEMENTATION -> ETLs END


# Cần implementation Spark code
# Cần resource aws -> aws resource management


job.commit()
