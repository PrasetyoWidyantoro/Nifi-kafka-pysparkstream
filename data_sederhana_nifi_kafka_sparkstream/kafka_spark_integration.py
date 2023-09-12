from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Inisialisasi sesi Spark
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .getOrCreate()

# Konfigurasi Kafka
kafkaParams = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "data_sederhana",  # Ganti dengan nama topik Kafka yang ingin Anda konsumsi.
    "startingOffsets": "earliest",
    "failOnDataLoss": "false"
}

# Definisikan skema untuk data JSON yang akan dibaca.
json_schema = StructType([
    StructField("nama", StringType(), True),
    StructField("pekerjaan", StringType(), True),
    StructField("gaji", DoubleType(), True)
])

# Baca data dari Kafka topic dalam format JSON.
raw_stream_data = spark \
    .readStream \
    .format("kafka") \
    .options(**kafkaParams) \
    .load()

# Ubah data JSON yang diterima ke dalam DataFrame.
parsed_stream_data = raw_stream_data \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", json_schema).alias("data"))

# Ambil kolom yang diperlukan dari DataFrame.
df = parsed_stream_data.select(col("data.nama"), col("data.pekerjaan"), col("data.gaji"))

# Tampilkan DataFrame yang telah diolah.
query = df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

#query = df \
#    .writeStream \
#    .outputMode("append") \
#    .format("console") \
#    .start()

query.awaitTermination()
