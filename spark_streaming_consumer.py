from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType


# Definir el esquema de los datos
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", FloatType()),
    StructField("humidity", FloatType()),
    StructField("timestamp", TimestampType())
])



# Crear sesiòn de Spark
spark = SparkSession.builder.appName("SensorDataAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Leer desde Kafka
df = spark.readStream.format("kafka") \
     .option("kafka.bootstrap.servers", "localhost:9092") \
     .option("subscribe", "sensor_data") \
     .load()


# Parsear los datos JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias
("data")).select("data.*")

# Calcular estadìsticas por ventana de tiempo
windowed_stats = parsed_df \
    .groupBy(window(col("timestamp"), "1 minute"), "sensor_id") \
    .agg({"temperature": "avg", "humidity": "avg"})

# Escribir resultados en consola
query = windowed_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
