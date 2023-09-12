from pyspark.sql import SparkSession
from pyspark.sql.functions import *

n_sec = 1
topic = "data_sederhana"

spark = SparkSession.builder \
    .appName("KafkaStreamProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Membaca data streaming dari Kafka topic
kafkaStream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .load()

# Mengurai pesan Kafka menjadi kolom-kolom
value = kafkaStream.selectExpr("CAST(value AS STRING)")
parsed_data = value.selectExpr(
    "get_json_object(value, '$.nama') as nama",
    "get_json_object(value, '$.pekerjaan') as pekerjaan",
    "cast(get_json_object(value, '$.gaji') as int) as gaji"
)

# Mengakumulasi (menjumlahkan) gaji untuk setiap nama
accumulated_data = parsed_data.groupBy("nama", "pekerjaan").agg(
    sum("gaji").alias("total_gaji")
)

# Menampilkan hasil streaming
query = accumulated_data.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()
