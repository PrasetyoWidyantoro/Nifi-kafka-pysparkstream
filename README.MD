# NIFI - KAFKA - PYSPARK STREAMING Sederhana

Repository ini dibuat untuk sarana belajar penulis untuk mengetahui cara bekerja dari apache nifi, kafka dan spark serta tujuan dokumentasi pribadi

Cara Runnning Spark Submit Pyspark Streaming pada folder `data_sederhana_nifi_kafka_sparkstream` adalah sebagai berikut:

* Jika hanya ingin menampilkan data dalam bentuk csvnya saja
*`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 "C:\Users\hp\Spark Training\kafka_spark_integration.py"`

* Jika ingin menampilkan data dalam bentuk akumulasi gaji masing masing pekerja
*`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 "C:\Users\hp\Spark Training\kafka_spark_integration_1.py"`
