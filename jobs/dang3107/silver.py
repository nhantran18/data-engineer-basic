import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from joblib.pull_api_utils import add_timestamp_to_filename
from joblib import variables as V

## @params: [JOB_NAME], Get the S3_INPUT_PATH from the arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH'])
print(args)


# Initialize Spark session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read the JSON file from S3
df = spark.read.option("multiline", "true").json(args['S3_INPUT_PATH'])



# Function to flatten the nested JSON data
def flatten_event(df):
    # Select and rename fields directly
    df_flatten = df.select(
        col("id").alias("event_id"),
        col("name").alias("event_name"),
        col("url").alias("event_url"),
        col("locale").alias("locale"),
        col("dates.start.dateTime").alias("event_date"),
        col("sales.public.startDateTime").alias("event_sales_start_date"),
        col("sales.public.endDateTime").alias("event_sales_end_date"),
        col("_embedded.venues").getItem(0).getField("id").alias("venue_id"),
        col("_embedded.venues").getItem(0).getField("name").alias("venue_name"),
        col("_embedded.venues").getItem(0).getField("address").getField("line1").alias("venue_address"),
        col("_embedded.venues").getItem(0).getField("city").getField("name").alias("venue_city"),
        col("_embedded.venues").getItem(0).getField("state").getField("stateCode").alias("venue_state"),
        col("_embedded.venues").getItem(0).getField("country").getField("countryCode").alias("venue_country"),
        col("_embedded.venues").getItem(0).getField("postalCode").alias("venue_postal_code"),
        col("_embedded.venues").getItem(0).getField("location").getField("latitude").alias("venue_latitude"),
        col("_embedded.venues").getItem(0).getField("location").getField("longitude").alias("venue_longitude"),
        col("seatmap.staticUrl").alias("seatmap_url"),
        col("promoter.name").alias("promoter_name"),
        col("promoter.id").alias("promoter_id"),
        col("_embedded.venues").getItem(0).getField("parkingDetail").alias("parking_info"),
        col("classifications").getItem(0).getField("segment").getField("id").alias("segment_id"),
        col("classifications").getItem(0).getField("segment").getField("name").alias("segment_name"),
        col("classifications").getItem(0).getField("genre").getField("id").alias("genre_id"),
        col("classifications").getItem(0).getField("genre").getField("name").alias("genre_name"),
        col("classifications").getItem(0).getField("subGenre").getField("id").alias("subGenre_id"),
        col("classifications").getItem(0).getField("subGenre").getField("name").alias("subGenre_name"),
        col("classifications").getItem(0).getField("type").getField("id").alias("type_id"),
        col("classifications").getItem(0).getField("type").getField("name").alias("type_name"),
        col("classifications").getItem(0).getField("subType").getField("id").alias("subType_id"),
        col("classifications").getItem(0).getField("subType").getField("name").alias("subType_name"),
        col("priceRanges").getItem(0).getField("currency").alias("price_currency"),
        col("priceRanges").getItem(0).getField("min").cast("double").alias("min_price"),
        col("priceRanges").getItem(0).getField("max").cast("double").alias("max_price"),
        col("_embedded.venues").getItem(0).getField("boxOfficeInfo").getField("phoneNumberDetail").alias("box_office_phone_number"),
        col("_embedded.venues").getItem(0).getField("boxOfficeInfo").getField("openHoursDetail").alias("box_office_open_hours"),
        col("_embedded.venues").getItem(0).getField("boxOfficeInfo").getField("acceptedPaymentDetail").alias("box_office_accepted_payment"),
        col("_embedded.venues").getItem(0).getField("boxOfficeInfo").getField("willCallDetail").alias("box_office_will_call_detail")
    )

    return df_flatten

# Flatten the DataFrame
flattened_df = flatten_event(df)


# Write the flattened DataFrame to a Parquet file
bucket_name = V.DATA_LANDING_BUCKET_NAME
object_key = add_timestamp_to_filename('silver_zone')
s3_saving_path = f's3://{bucket_name}/dang3107/silver/{object_key}'

# Write to S3 silver bucket
flattened_df.write.mode('overwrite').parquet(s3_saving_path)

print(f'Successfully uploaded the Parquet to {s3_saving_path}')

job.commit()