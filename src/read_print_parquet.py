import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, LongType, StringType, StructField,
                               StructType, TimestampType)

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "traffic_sensor"
FILE_PATH = "/data/AGOSTO_2022_PARQUET_FINAL/"

# "ID EQP" -> INT 64

SCHEMA = StructType([
    StructField("ID EQP", LongType()),                  # Equip. ID
    StructField("DATA HORA", TimestampType()),          # Date and hour
    StructField("MILESEGUNDO", LongType()),             # Millissecond
    StructField("CLASSIFICAÇÃO", StringType()),         # Classification (Motocycle, Car, bus/truck)
    StructField("FAIXA", LongType()),                   # Road lane
    StructField("ID DE ENDEREÇO", LongType()),          # Address ID
    StructField("VELOCIDADE DA VIA", StringType()),     # Road speed
    StructField("VELOCIDADE AFERIDA", StringType()),    # Measured vehicle speed
    StructField("TAMANHO", StringType()),               # Vehicle lenght
    StructField("NUMERO DE SÉRIE", LongType()),         # Serial number
    StructField("LATITUDE", StringType()),              # Latitude
    StructField("LONGITUDE", StringType()),             # Longitude
    StructField("ENDEREÇO", StringType()),              # Address
    StructField("SENTIDO", StringType())                # Direction
])

spark = SparkSession.builder.appName("write_traffic_sensor_topic").getOrCreate()
spark.sparkContext.setLogLevel("WARN") # Reduce logging verbosity

# Read the parquet file write it to the topic
# We need to specify the schema in the stream
# and also convert the entries to the format key, value
df_traffic_stream = spark.readStream.format("parquet")\
    .schema(SCHEMA)\
    .load(FILE_PATH)\
    .withColumn("value", F.to_json( F.struct(F.col("*")) ) )\
    .withColumn("key", F.lit("key"))\
    .withColumn("value", F.encode(F.col("value"), "iso-8859-1").cast("binary"))\
    .withColumn("key", F.encode(F.col("key"), "iso-8859-1").cast("binary"))\
    .limit(10)\

# Write the stream to the topic
df_traffic_stream\
    .writeStream\
    .format("console")\
    .option("truncate", "false")\
    .outputMode("append")\
    .start()\
    .awaitTermination()