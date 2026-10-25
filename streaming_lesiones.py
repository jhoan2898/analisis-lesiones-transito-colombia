from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("StreamingLesionesTransito") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

esquema = StructType([
    StructField("FECHA HECHO", StringType(), True),
    StructField("COD_DEPTO", StringType(), True),
    StructField("DEPARTAMENTO", StringType(), True),
    StructField("COD_MUNI", StringType(), True),
    StructField("MUNICIPIO", StringType(), True),
    StructField("CANTIDAD", StringType(), True)
])

print("Iniciando Spark Streaming para lesiones de tránsito...", flush=True)

flujo = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "lesiones_transito") \
    .option("startingOffsets", "earliest") \
    .load()

mensajes = flujo.selectExpr("CAST(value AS STRING) AS mensaje")

datos = mensajes.select(
    from_json(col("mensaje"), esquema).alias("registro")
).select("registro.*")

consulta = datos.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("numRows", 10) \
    .start()

consulta.awaitTermination()
