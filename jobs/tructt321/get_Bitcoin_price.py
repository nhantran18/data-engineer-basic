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

# Lựa chọn các cột 'name', 'quote.USD.price', và 'timestamp' từ DataFrame df72
df2 = df_rawdata.select("name", "`quote.USD.price`", "timestamp")

# Lọc các hàng có 'name' là 'Bitcoin'
df_bitcoin = df2.filter(df2["name"] == "Bitcoin")

# Hiển thị DataFrame
df_bitcoin.show()

# Tạo một đường dẫn mới với timestamp (ngày giờ hiện tại)
destination_path = f"s3://ai4e-ap-southeast-1-dev-s3-data-landing/tructt321/bitcoin/Bitcoin_price.parquet"

# Lưu DataFrame vào file Parquet mới

df_bitcoin.write.mode('append').option("header", "true").csv(destination_path)

print(f"Data appended to {destination_path}")
#### END JOB DIMENTION IMPLEMENTATION -> ETLs END


# Cần implementation Spark code
# Cần resource aws -> aws resource management


job.commit()
