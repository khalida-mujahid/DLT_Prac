from pyspark.sql.functions import (
    col, count, count_if, from_json, explode, from_unixtime, current_timestamp
)
import dlt
from pyspark.sql.types import *

catalog_name = spark.conf.get('catalog_name')

volume_path = f'/Volumes/{catalog_name}/youtubebronze/earthquake_data'
primary_key = "id"

properties_schema = StructType([
    StructField("mag", DoubleType()),
    StructField("place", StringType()),
    StructField("time", LongType()),
    StructField("updated", LongType()),
    StructField("tz", IntegerType()),
    StructField("url", StringType()),
    StructField("detail", StringType()),
    StructField("felt", IntegerType()),
    StructField("cdi", DoubleType()),
    StructField("mmi", DoubleType()),
    StructField("alert", StringType()),
    StructField("status", StringType()),
    StructField("tsunami", IntegerType()),
    StructField("sig", IntegerType()),
    StructField("net", StringType()),
    StructField("code", StringType()),
    StructField("ids", StringType()),
    StructField("sources", StringType()),
    StructField("types", StringType()),
    StructField("nst", IntegerType()),
    StructField("dmin", DoubleType()),
    StructField("rms", DoubleType()),
    StructField("gap", IntegerType()),
    StructField("magType", StringType()),
    StructField("type", StringType()),
    StructField("title", StringType())
])

geometry_schema = StructType([
    StructField("coordinates", ArrayType(DoubleType())),
    StructField("type", StringType())
])

feature_schema = StructType([
    StructField("id", StringType()),
    StructField("properties", properties_schema),
    StructField("geometry", geometry_schema),
    StructField("type", StringType())
])

schema = ArrayType(feature_schema)

@dlt.view(name='earthquake_data_vw')
def earthquake_data():
    df = (
        spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format', 'json')
        .load(volume_path)
        .withColumn("ingest_time", current_timestamp())
    )
    df = df.withColumn("parsed_data", from_json(col("features"), schema))
    df = df.select(explode(col("parsed_data")).alias("features"), "ingest_time")
    df = df.select(
        "features.properties.*",
        "features.id",
        col("features.geometry.coordinates").getItem(0).alias("longitude"),
        col("features.geometry.coordinates").getItem(1).alias("latitude"),
        col("features.geometry.coordinates").getItem(2).alias("depth"),
        "ingest_time"
    )
    df = df.withColumn(
        "time", from_unixtime(col("time")/1000).cast("timestamp")
    ).withColumn(
        "updated", from_unixtime(col("updated")/1000).cast("timestamp")
    )
    return df

dlt.create_streaming_table(name="earthquake_data_final")
dlt.expect_or_fail("unique_id", "num_entries = 1")
dlt.apply_changes(
    target="earthquake_data_final",
    source="earthquake_data_vw",
    keys=[primary_key],
    sequence_by="ingest_time",
    stored_as_scd_type='1'
)