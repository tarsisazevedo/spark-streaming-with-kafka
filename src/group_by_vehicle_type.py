import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (LongType, StringType, StructField, StructType,
                               TimestampType)

spark = SparkSession.builder\
.appName('gb_vehicle_type')\
.getOrCreate()

# Reduce logging verbosity
spark.sparkContext.setLogLevel("WARN")

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "traffic_sensor"
SCHEMA = StructType([
    StructField("ID EQP", LongType()),
    StructField("DATA HORA", TimestampType()),
    StructField("MILESEGUNDO", LongType()),
    StructField("CLASSIFICAÇÃO", StringType()),
    StructField("FAIXA", LongType()),
    StructField("ID DE ENDEREÇO", LongType()),
    StructField("VELOCIDADE DA VIA", StringType()),
    StructField("VELOCIDADE AFERIDA", StringType()),
    StructField("TAMANHO", StringType()),
    StructField("NUMERO DE SÉRIE", LongType()),
    StructField("LATITUDE", StringType()),
    StructField("LONGITUDE", StringType()),
    StructField("ENDEREÇO", StringType()),
    StructField("SENTIDO", StringType())
])

df_traffic_stream = spark\
    .readStream.format("kafka")\
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)\
    .option("subscribe", KAFKA_TOPIC)\
    .option("startingOffsets", "earliest")\
    .load()\
    .select(
    F.from_json(
        F.decode(F.col("value"), "iso-8859-1"),
        SCHEMA
    ).alias("value")
    )\
    .select("value.*")

# Count the total number of records in the 30 seconds tumbling window
df_traffic_stream\
    .groupBy(
        F.window("DATA HORA", "10 minutes"),
        F.col("CLASSIFICAÇÃO")
    )\
    .count()\
    .writeStream\
    .option("truncate", "false")\
    .outputMode("update")\
    .format("console")\
    .start()\
    .awaitTermination()

# # Count the total number of records in the 30 seconds tumbling window grouped by vehicle type
# df_traffic_stream\
#     .groupBy(
#         F.window("DATA HORA", "30 seconds"),
#         F.col("CLASSIFICAÇÃO")
#     )\
#     .count()\
#     .writeStream\
#     .option("truncate", "false")\
#     .outputMode("update")\
#     .format("console")\
#     .start()\
#     .awaitTermination()

# Count total number of records in the 30 minutes window every 30 seconds grouped by vehicle type
# df_traffic_stream\
#     .groupBy(
#         F.window("DATA HORA", "30 minutes", "30 seconds")
#     )\
#     .count()\
#     .orderBy("window")\
#     .writeStream\
#     .option("truncate", "false")\
#     .outputMode("complete")\
#     .format("console")\
#     .option("truncate", "false")\
#     .start()\
#     .awaitTermination()