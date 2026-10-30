from pyspark.sql import SparkSession, functions as F


spark = SparkSession.builder.appName("BatchAnalysis").getOrCreate()
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'
df = spark.read.format('csv').option('header', 'true').option('inferSechem', 'true').load(file_path)

df_clean = df.na.drop()

print("=== ESQUEMA DEL DATASET ===")
df_clean.printSchema()


print("\n==== PRIMEROS 5 REGISTROS ===")
df_clean.show(5)


print("\n==== ESTADÌSTICAS BÀSCIAS ===")
df_clean.summary().show()


print("\n=== VALORES MAYORES A 5000 COP ===")
df_clean.filter(F.col('valor').cast('double') > 5000).select('VALOR', 'VIGENCIADESDE', 'VIGENCIAHASTA').show()


print("\n=== TOP 10 VALORES MÀS ALTOS ===")
df_clean.sort(F.col('VALOR').desc()).show(10)
