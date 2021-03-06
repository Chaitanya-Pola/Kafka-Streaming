import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources.
# defining the structure of the input file
schema = StructType([
    StructField('crime_id', StringType()),
    StructField('original_crime_type_name', StringType()),
    StructField('report_date', StringType()),
    StructField('call_date', StringType()),
    StructField('offense_date', StringType()),
    StructField('call_time', StringType()),
    StructField('call_date_time', StringType()),
    StructField('disposition', StringType()),
    StructField('address', StringType()),
    StructField('city', StringType()),
    StructField('state', StringType()),
    StructField('agency_id', StringType()),
    StructField('address_type', StringType()),
    StructField('common_location', StringType())])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    # Creating the dataframe by reading the input spark stream
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "police.dept.service.log") \
        .option("startingOffsets", "earliest").option("maxOffsetsPerTrigger", "200").option("maxRatePerPartition", "2") \
        .option("stopGracefullyOnShutdown", "true").load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(VALUE AS STRING)")

    service_table = kafka_df.select(psf.from_json(psf.col('value'), schema).alias("DF")).select("DF.*")
    
    print("schema of the service table")
    service_table.printSchema()
    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select(psf.to_timestamp(psf.col("call_date_time")).alias("call_date_time"),psf.col("original_crime_type_name"),
        psf.col("disposition"))
    
    print("schema of distinct table")
    distinct_table.printSchema()
    # count the number of original crime type
    
    aggregate_DF = distinct_table.select(distinct_table.call_date_time,distinct_table.original_crime_type_name,distinct_table.disposition)\
    .withWatermark("call_date_time", "60 minutes") \
    .groupBy(psf.window(distinct_table.call_date_time, "10 minutes", "5 minutes"),psf.col("original_crime_type_name")).count()
    

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = aggregate_DF.writeStream.format('console').outputMode('Complete').trigger(processingTime="10 seconds").start()


    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = aggregate_DF.join(radio_code_df,col("agg_df.disposition") == col("radio_code_df.disposition"), "left_outer")


    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession.builder.config("spark.ui.port", 3000).master("local[*]").appName("KafkaSparkStructuredStreaming").getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
