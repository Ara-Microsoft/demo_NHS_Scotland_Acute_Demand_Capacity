from pyspark import pipelines as dp
from datetime import datetime
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    DoubleType,
)
from pyspark.sql.functions import col, count, count_if, explode, from_json, current_timestamp, from_unixtime
from utilities import utils
import dlt


catalog_name = spark.conf.get('catalog_name')
schema_name = spark.conf.get('schema_name')


volume_path = f"/Volumes/{catalog_name}/{schema_name}/earthquake_volume/*.json"

primary_key = 'id'

properties_schema = StructType(
    [
        StructField("mag", StringType()),
        StructField("place", StringType()),
        StructField("time", StringType()),
        StructField("status", StringType()),
        StructField("tsunami", StringType()),
        StructField("type", StringType()),
        StructField("url", StringType()),
        StructField("detail", StringType()),
        StructField("felt", StringType()),
        StructField("cdi", StringType()),
        StructField("mmi", StringType()),
        StructField("alert", StringType()),
        StructField("sig", StringType()),
        StructField("net", StringType()),
        StructField("code", StringType()),
        StructField("ids", StringType()),
        StructField("sources", StringType()),
        StructField("types", StringType()),
        StructField("nst", StringType()),
        StructField("dmin", StringType()),
        StructField("rms", StringType()),
        StructField("gap", StringType()),
        StructField("magType", StringType()),
        StructField("title", StringType()),
    ]
)

geometry_schema = StructType([StructField("coordinates", ArrayType(DoubleType()))])

feature_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("properties", properties_schema),
        StructField("geometry", geometry_schema),
    ]
)

schema = ArrayType(feature_schema)


@dlt.table(name="earthquake_data")
def clean_bronze_data():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(volume_path)
    )
    return df


@dlt.table(name="earthquake_data_clean")
def clean_bronze_data():

    df = spark.read.table("earthquake_data").withColumn(
        "load_timestamp", current_timestamp()
    )
    df = df.withColumn("parsed_data", explode(from_json(col("features"), schema)))
    df = df.select('load_timestamp',
        "parsed_data.id", "parsed_data.geometry.coordinates", "parsed_data.properties.*"
    )
    df = (
        df.withColumn(
            "time", from_unixtime(col("time").cast("bigint"), "yyyy-MM-dd HH:mm:ss")
        )
        .withColumn("mag", col("mag").cast("double"))
        .withColumn("nst", col("nst").cast("double"))
        .withColumn("tsunami", col("tsunami").cast("double"))
        .withColumn("sig", col("sig").cast("double"))
        .withColumn("dmin", col("dmin").cast("double"))
        .withColumn("rms", col("rms").cast("double"))
        .withColumn("gap", col("gap").cast("double"))
        .withColumn("magType", col("magType").cast("double"))
        .withColumn("Longitude", col("coordinates")[0])
        .withColumn("Latitude", col("coordinates")[1])
    )

    return df


dlt.create_streaming_table(name="eathquake_data_final")

dlt.apply_changes(
        target='eathquake_data_final',
        source = 'earthquake_data_clean',
        keys = [primary_key],
        sequence_by = 'load_timestamp',
        stored_as_scd_type='1'

    )