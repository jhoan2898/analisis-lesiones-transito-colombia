# Análisis de lesiones por accidentes de tránsito en Colombia

## Descripción
Proyecto académico de Big Data de la UNAD que utiliza Apache Spark, Apache Kafka y Spark Streaming para analizar datos relacionados con lesiones por accidentes de tránsito en Colombia.

## Conjunto de datos
Archivo CSV obtenido de Datos Abiertos de Colombia. Contiene fecha del hecho, códigos y nombres de departamentos y municipios, y la variable CANTIDAD.

## Tecnologías
- Python
- Apache Spark 3.5.8
- Apache Kafka 3.9.2
- Spark Structured Streaming
- kafka-python

## Programas
- `analisis_lesiones.py`: procesa los datos por lotes, calcula estadísticas y agrupa los resultados por departamento y municipio.
- `productor_lesiones.py`: envía los registros del archivo CSV al tópico `lesiones_transito` de Kafka.
- `streaming_lesiones.py`: consume los mensajes de Kafka mediante Spark Structured Streaming y muestra los registros recibidos.

## Resultados
El procesamiento por lotes analizó 644.042 registros. La suma de la variable CANTIDAD por departamento presentó los valores más altos en Valle del Cauca, Bogotá D. C. y Antioquia.

## Ejecución

### Procesamiento por lotes
```bash
spark-submit ~/proyecto_lesiones/analisis_lesiones.py
