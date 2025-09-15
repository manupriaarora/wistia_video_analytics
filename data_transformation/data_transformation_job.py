import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit
from datetime import datetime


# --- Initialization ---
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

destination_path = "s3://wistia-video-analytics-de-project/wistia-pipeline/transformed/"

# ---------------------------
# 1 Incremental tables (DynamicFrame + Glue Job Bookmarks)
# ---------------------------
def process_incremental_table(db_name, table_name, target_subfolder):
    print(f"Processing incremental table: {table_name}...")
    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=db_name,
        table_name=table_name,
        transformation_ctx=f"{table_name}_datasource"
    )
    
    df = dyf.toDF()
    if df.count() == 0:
        print(f"No new data for {table_name}. Skipping...")
        return
    
    # Flatten nested data if any
    relationalized = DynamicFrame.fromDF(df, glueContext, f"{table_name}_df").relationalize("root", "./")
    if "root" in relationalized.keys():
        df_flat = relationalized["root"].toDF()
    else:
        df_flat = df  # fallback
    
    dyf_transformed = DynamicFrame.fromDF(df_flat, glueContext, f"{table_name}_transformed")
    
    glueContext.write_dynamic_frame.from_options(
        frame=dyf_transformed,
        connection_type="s3",
        connection_options={"path": destination_path + target_subfolder, "partitionKeys": []},
        format="parquet",
        transformation_ctx=f"{table_name}_datasink"
    )

# Incremental tables
process_incremental_table("wistia_loading_zone_db", "event_stats", "event_stats")
process_incremental_table("wistia_loading_zone_db", "visitor_stats", "visitor_stats")

# ---------------------------
# 2 Snapshot tables (DataFrame + overwrite mode)
# ---------------------------
def write_snapshot_table(dyf, target_subfolder, table_name):
    df = dyf.toDF()
    
    # Flatten only if nested
    if any([f.dataType.typeName() == "struct" for f in df.schema.fields]):
        relationalized = dyf.relationalize("root", "./")
        if "root" in relationalized.keys():
            df = relationalized["root"].toDF()
        else:
            print(f"No 'root' table in {table_name}, using original DF.")
    
    # Skip empty DataFrames
    if len(df.columns) == 0 or df.rdd.isEmpty():
        print(f"No data to write for {table_name}. Skipping...")
        return
    
    snapshot_date = datetime.now().strftime("%Y-%m-%d")
    df = df.withColumn("snapshot_date", lit(snapshot_date))
    
    print(f"Writing snapshot for {table_name} to S3 (append mode)...")
    df.write.mode("append").partitionBy("snapshot_date").parquet(destination_path + target_subfolder)

# Snapshot tables
media_stats_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="wistia_loading_zone_db",
    table_name="media_stats",
    transformation_ctx="media_stats_datasource"
)
write_snapshot_table(media_stats_dyf, "media_stats", "media_stats")

media_show_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="wistia_loading_zone_db",
    table_name="media_show",
    transformation_ctx="media_show_datasource"
)
write_snapshot_table(media_show_dyf, "media_show", "media_show")

# --- Job Completion ---
job.commit()
print("Glue job completed successfully.")
