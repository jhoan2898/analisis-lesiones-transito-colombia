from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, desc, to_date

spark = SparkSession.builder \
    .appName("AnalisisLesionesTransitoColombia") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

ruta = "/home/hadoop/LESIONES_ACCIDENTES_DE_TRÁNSITO_20261025.csv"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("encoding", "UTF-8") \
    .option("delimiter", ",") \
    .csv(ruta)

print("\n========== ESQUEMA DEL DATASET ==========")
df.printSchema()

print("\n========== TOTAL DE REGISTROS ==========")
print("Total:", df.count())

print("\n========== PRIMERAS 10 FILAS ==========")
df.show(10, truncate=False)

print("\n========== ESTADISTICAS DESCRIPTIVAS ==========")
df.select("CANTIDAD").describe().show()

print("\n========== REGISTROS POR DEPARTAMENTO ==========")
df.groupBy("DEPARTAMENTO") \
    .agg(
        count("*").alias("NUM_REGISTROS"),
        spark_sum("CANTIDAD").alias("SUMA_CANTIDAD")
    ) \
    .orderBy(desc("SUMA_CANTIDAD")) \
    .show(15, truncate=False)

print("\n========== REGISTROS POR MUNICIPIO ==========")
df.groupBy("DEPARTAMENTO", "MUNICIPIO") \
    .agg(spark_sum("CANTIDAD").alias("SUMA_CANTIDAD")) \
    .orderBy(desc("SUMA_CANTIDAD")) \
    .show(15, truncate=False)

print("\n========== REGISTROS POR FECHA ==========")
df.groupBy("FECHA HECHO") \
    .agg(spark_sum("CANTIDAD").alias("SUMA_CANTIDAD")) \
    .orderBy(desc("SUMA_CANTIDAD")) \
    .show(10, truncate=False)

print("\n========== REGISTROS CON CANTIDAD MAYOR QUE CERO ==========")
df.filter(col("CANTIDAD") > 0) \
    .select("FECHA HECHO", "DEPARTAMENTO", "MUNICIPIO", "CANTIDAD") \
    .show(10, truncate=False)

salida = "/home/hadoop/resultados_lesiones"
df.groupBy("DEPARTAMENTO") \
    .agg(spark_sum("CANTIDAD").alias("SUMA_CANTIDAD")) \
    .orderBy(desc("SUMA_CANTIDAD")) \
    .write.mode("overwrite").option("header", "true").csv(salida)

print("\nResultados guardados en:", salida)

spark.stop()

