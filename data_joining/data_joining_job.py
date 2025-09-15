import logging
import sys
import boto3
from botocore.exceptions import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when, col

# --- Global Initialization ---
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Initialize the S3 client
s3_client = boto3.client('s3')

# --- Configuration and Initialization ---
# Initialize the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --- Configuration ---
TARGET_MEDIA_IDS = ["gskhw4w4lm", "v08dlrgr7v"]
TARGET_MEDIA_COLUMNS = ["media_id", "title", "url", "created_at"]
TARGET_VISITOR_COLUMNS = ["visitor_id", "ip_address", "country"]
TARGET_FACT_COLUMNS = [
    "media_id",
    "visitor_id",
    "date",
    "play_count",
    "load_count",
    "play_rate",
    "total_watch_time",
    "watched_percent"
]

# S3 bucket for the joined data
S3_BUCKET_NAME = "wistia-video-analytics-de-project"

def empty_s3_prefix(bucket, prefix):
    """
    Deletes all objects within a specific S3 prefix. This is the "truncate" part
    of a truncate-and-load operation.
    """
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        objects_to_delete = []
        for page in pages:
            if "Contents" in page:
                for obj in page['Contents']:
                    objects_to_delete.append({'Key': obj['Key']})

        if objects_to_delete:
            s3_client.delete_objects(Bucket=bucket, Delete={'Objects': objects_to_delete})
            logger.info(f"Successfully deleted {len(objects_to_delete)} objects from s3://{bucket}/{prefix}")
        else:
            logger.info(f"No objects found to delete in s3://{bucket}/{prefix}")

    except ClientError as e:
        logger.info(f"Error deleting objects from S3: {e}")
    except Exception as e:
        logger.info(f"An unexpected error occurred during S3 deletion: {e}")


# --- Data Reading and Transformation ---
logger.info("Reading event_stats table...")
event_stats = glueContext.create_dynamic_frame.from_catalog(
    database="wistia_transformation_zone_db", 
    table_name="event_stats", 
    transformation_ctx="event_stats"
)

logger.info("Reading media_show table...")
media_show = glueContext.create_dynamic_frame.from_catalog(
    database="wistia_transformation_zone_db", 
    table_name="media_show",
    transformation_ctx="media_show"
)

logger.info("Reading visitor_stats table...")
visitor_stats = glueContext.create_dynamic_frame.from_catalog(
    database="wistia_transformation_zone_db", 
    table_name="visitor_stats", 
    transformation_ctx="visitor_stats"
)

logger.info("Converting to Spark DataFrames to perform a left join...")
event_stats_df = event_stats.toDF()
media_show_df = media_show.toDF()
visitor_stats_df = visitor_stats.toDF()

# --- Creating Table 1: dim_media ---
logger.info("Creating dim_media table...")
media_show_df = media_show_df.withColumnRenamed("hashed_id", "media_id").withColumnRenamed("name", "title").withColumnRenamed("created", "created_at")
event_stats_df = event_stats_df.withColumnRenamed("media_url", "url").withColumnRenamed("received_at", "date").withColumnRenamed("percent_viewed", "watched_percent")

logger.info("Performing left join on 'media_id'...")
media_joined_df = media_show_df.join(event_stats_df, on="media_id", how="left")

logger.info(f"Filtering joined data for media IDs: {TARGET_MEDIA_IDS}")
media_joined_df = media_joined_df.filter(media_joined_df.media_id.isin(TARGET_MEDIA_IDS))

logger.info("Adding 'channel' column based on media_id...")
media_joined_df = media_joined_df.withColumn(
    "channel",
    when(col("media_id") == "gskhw4w4lm", "YouTube").otherwise("Facebook")
)

logger.info(f"Selecting specific columns: {TARGET_MEDIA_COLUMNS} and 'channel'")
media_joined_df = media_joined_df.select(*TARGET_MEDIA_COLUMNS, col("channel"))

# Get distinct rows to avoid duplicates in the dimension table
logger.info("Getting distinct rows to ensure unique visitors...")
media_joined_df = media_joined_df.distinct()

media_joined_data = DynamicFrame.fromDF(
    media_joined_df, 
    glueContext, 
    "media_joined"
)

# --- Data Sinking for dim_media (Truncate and Load) ---
destination_prefix_media = "wistia-pipeline/joined/dim_media/"
logger.info(f"Starting 'truncate and load' for dim_media at: s3://{S3_BUCKET_NAME}/{destination_prefix_media}")

# Step 1: Truncate (delete all existing files)
empty_s3_prefix(S3_BUCKET_NAME, destination_prefix_media)

# Step 2: Load (write the new data)
logger.info("Writing dim_media data to S3...")
glueContext.write_dynamic_frame.from_options(
    frame=media_joined_data,
    connection_type="s3",
    connection_options={
        "path": f"s3://{S3_BUCKET_NAME}/{destination_prefix_media}",
        "partitionKeys": []
    },
    format="parquet",
    transformation_ctx="media_datasink"
)

# --- Creating Table 2: dim_visitor ---
logger.info("\nCreating dim_visitor table...")
logger.info("Performing left join on 'visitor_key'...")
visitor_joined_df = visitor_stats_df.join(event_stats_df, on="visitor_key", how="left")

visitor_joined_df = visitor_joined_df.withColumnRenamed("visitor_key", "visitor_id").withColumnRenamed("ip", "ip_address")

logger.info(f"Filtering joined data for media IDs: {TARGET_MEDIA_IDS}")
visitor_joined_df = visitor_joined_df.filter(visitor_joined_df.media_id.isin(TARGET_MEDIA_IDS))

logger.info(f"Selecting specific columns: {TARGET_VISITOR_COLUMNS}")
visitor_joined_df = visitor_joined_df.select(*TARGET_VISITOR_COLUMNS)

logger.info("Getting distinct rows to ensure unique visitors...")
visitor_joined_df = visitor_joined_df.distinct()

visitor_joined_data = DynamicFrame.fromDF(
    visitor_joined_df, 
    glueContext, 
    "visitor_joined"
)

# --- Data Sinking for dim_visitor (Truncate and Load) ---
destination_prefix_visitor = "wistia-pipeline/joined/dim_visitor/"
logger.info(f"Starting 'truncate and load' for dim_visitor at: s3://{S3_BUCKET_NAME}/{destination_prefix_visitor}")

# Step 1: Truncate (delete all existing files)
empty_s3_prefix(S3_BUCKET_NAME, destination_prefix_visitor)

# Step 2: Load (write the new data)
logger.info("Writing dim_visitor data to S3...")
glueContext.write_dynamic_frame.from_options(
    frame=visitor_joined_data,
    connection_type="s3",
    connection_options={
        "path": f"s3://{S3_BUCKET_NAME}/{destination_prefix_visitor}",
        "partitionKeys": []
    },
    format="parquet",
    transformation_ctx="visitor_datasink"
)

# --- Creating Fact Table: fact_media_engagement ---
logger.info("\nCreating Fact Table: fact_media_engagement...")

media_show_df = media_show_df.dropDuplicates()
logger.info("Joining event_stats with media_show on 'media_id'...")
fact_table_df = event_stats_df.join(media_show_df, on="media_id", how="left")

logger.info("Joining with visitor_stats on 'visitor_id'...")
visitor_stats_df = visitor_stats_df.dropDuplicates()
fact_table_df = fact_table_df.join(visitor_stats_df, on="visitor_key", how="left")

fact_table_df = fact_table_df.withColumnRenamed("visitor_key", "visitor_id")

logger.info(f"Filtering fact data for media IDs: {TARGET_MEDIA_IDS}")
fact_table_df = fact_table_df.filter(fact_table_df.media_id.isin(TARGET_MEDIA_IDS))

logger.info("Adding 'play_rate' column...")
fact_table_df = fact_table_df.withColumn(
    "play_rate",
    when(col("load_count") > 0, col("play_count") / col("load_count")).otherwise(0)
)

logger.info("Adding 'total_watch_time' column...")
fact_table_df = fact_table_df.withColumn(
    "total_watch_time",
    col("watched_percent") * col("duration")
)

logger.info(f"Selecting fact table columns: {TARGET_FACT_COLUMNS}")
fact_table_df = fact_table_df.select(*TARGET_FACT_COLUMNS)
fact_table_df = fact_table_df.dropDuplicates()
fact_table_data = DynamicFrame.fromDF(
    fact_table_df, 
    glueContext, 
    "fact_table"
)

# --- Data Sinking for Fact Table (Truncate and Load) ---
destination_prefix_fact = "wistia-pipeline/joined/fact_media_engagement/"
logger.info(f"Starting 'truncate and load' for fact_media_engagement at: s3://{S3_BUCKET_NAME}/{destination_prefix_fact}")

# Step 1: Truncate (delete all existing files)
empty_s3_prefix(S3_BUCKET_NAME, destination_prefix_fact)

# Step 2: Load (write the new data)
logger.info("Writing fact table data to S3...")
glueContext.write_dynamic_frame.from_options(
    frame=fact_table_data,
    connection_type="s3",
    connection_options={
        "path": f"s3://{S3_BUCKET_NAME}/{destination_prefix_fact}",
        "partitionKeys": []
    },
    format="parquet",
    transformation_ctx="fact_datasink"
)

job.commit()
logger.info("Glue job completed successfully.")
