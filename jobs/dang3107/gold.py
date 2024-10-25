import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, to_timestamp, lit
from pyspark.sql.types import IntegerType
from joblib.pull_api_utils import add_timestamp_to_filename
from joblib import variables as V
from datetime import datetime


## @params: [JOB_NAME], Get the S3_INPUT_PATH from the arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH'])
print(args)


# Initialize Spark session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Load the data
df = spark.read.format('parquet').load(args['INPUT_PATH']).cache()

# Data Cleaning and Transformation
df.printSchema()

# Remove empty event_date, min_price, max_price
df_cleaned = df.filter(col("event_date",).isNotNull())
df_cleaned = df_cleaned.filter(col("min_price").isNotNull())
df_cleaned = df_cleaned.filter(col("max_price").isNotNull())


# Define the date format
date_format = "yyyy-MM-dd'T'HH:mm:ss'Z'"

# Standardize date columns from string to timestamp using the specified format
df_cleaned = df_cleaned.withColumn("event_date", to_timestamp(col("event_date"), date_format))
df_cleaned = df_cleaned.withColumn("event_sales_start_date", to_timestamp(col("event_sales_start_date"), date_format))
df_cleaned = df_cleaned.withColumn("event_sales_end_date", to_timestamp(col("event_sales_end_date"), date_format))

# Cast price fields to float
df_cleaned = df_cleaned.withColumn("min_price", col("min_price").cast("float"))
df_cleaned = df_cleaned.withColumn("max_price", col("max_price").cast("float"))


# Replace null values in 'promoter_name' with 'Unknown'
df_cleaned = df_cleaned.withColumn("promoter_name", when(col("promoter_name").isNull(), "Unknown").otherwise(col("promoter_name")))


# Add a new column for the duration of the event sales period
df_cleaned = df_cleaned.withColumn("sales_duration_days", 
                   ((col("event_sales_end_date").cast("long") - col("event_sales_start_date").cast("long")) / (60 * 60 * 24)).cast(IntegerType()))

# Add a new column for average price
df_cleaned = df_cleaned.withColumn("average_price", (col("min_price") + col("max_price")) / 2)

# Add a new column for current date
current_date = lit(datetime.now().strftime("%Y-%m-%d")) 
df_cleaned = df_cleaned.withColumn("current_date", current_date)

# df_cleaned.show()

#TODO: Glue aggregate: thành phố có bn concert, events 

# 1. Event Dimension Table
event_df = df_cleaned.select("event_id", "event_name", "event_url", "locale", "current_date" ) \
    .dropDuplicates()

# 2. Venue Dimension Table
venue_df = df_cleaned.select("venue_id", "venue_name", "venue_address", "venue_city", "venue_state", 
                             "venue_country", "venue_postal_code", "venue_latitude", "venue_longitude", "seatmap_url", "parking_info", "current_date") \
    .dropDuplicates()

# 3. Segment Dimension Table
segment_df = df_cleaned.select("segment_id", "segment_name", "current_date") \
    .dropDuplicates()

# 4. Genre Dimension Table
genre_df = df_cleaned.select("genre_id", "genre_name", "current_date") \
    .dropDuplicates()

# 5. SubGenre Dimension Table
sub_genre_df = df_cleaned.select("subGenre_id", "subGenre_name", "current_date") \
    .dropDuplicates()

# 6. Type Dimension Table
type_df = df_cleaned.select("type_id", "type_name", "current_date") \
    .dropDuplicates()

# 7. SubType Dimension Table
sub_type_df = df_cleaned.select("subType_id", "subType_name", "current_date") \
    .dropDuplicates()

# 8. Promoter Dimension Table
promoter_df = df_cleaned.select("promoter_id", "promoter_name", "current_date") \
    .dropDuplicates()

# 9. Fact Table: Event Sales Fact Table
fact_df = df_cleaned.select(
    "event_id", "venue_id", "segment_id", "genre_id", "subGenre_id", 
    "type_id", "subType_id", "promoter_id", "box_office_phone_number", "box_office_open_hours", 
    "box_office_accepted_payment", "box_office_will_call_detail", "min_price", "max_price", "average_price", "price_currency", 
    "event_date", "sales_duration_days", "event_sales_start_date", "event_sales_end_date", "current_date"
)





# Write the transformed data to gold Parquet
object_key = add_timestamp_to_filename(V.GOLDEN_ZONE)
bucket_name = V.DATA_LANDING_BUCKET_NAME
s3_saving_path = f's3://{bucket_name}/dang3107/gold'

# Write to S3 gold bucket

fact_df.write.mode("overwrite").partitionBy("current_date").parquet(f"{s3_saving_path}/fact_events")
event_df.write.mode("overwrite").partitionBy("current_date").parquet(f"{s3_saving_path}/dim_event")
venue_df.write.mode("overwrite").partitionBy("current_date").parquet(f"{s3_saving_path}/dim_venue")
segment_df.write.mode("overwrite").partitionBy("current_date").parquet(f"{s3_saving_path}/dim_segment")
genre_df.write.mode("overwrite").partitionBy("current_date").parquet(f"{s3_saving_path}/dim_genre")
sub_genre_df.write.mode("overwrite").partitionBy("current_date").parquet(f"{s3_saving_path}/dim_subGenre")
type_df.write.mode("overwrite").partitionBy("current_date").parquet(f"{s3_saving_path}/dim_type")
sub_type_df.write.mode("overwrite").partitionBy("current_date").parquet(f"{s3_saving_path}/dim_subType")
promoter_df.write.mode("overwrite").partitionBy("current_date").parquet(f"{s3_saving_path}/dim_promoter")

print(f'Successfully uploaded the Parquet to {s3_saving_path}')


job.commit()