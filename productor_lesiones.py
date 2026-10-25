import csv
import json
import time
from kafka import KafkaProducer

ARCHIVO = "/home/hadoop/LESIONES_ACCIDENTES_DE_TRÁNSITO_20261025.csv"
SERVIDOR_KAFKA = "localhost:9092"
TEMA = "lesiones_transito"

productor = KafkaProducer(
    bootstrap_servers=[SERVIDOR_KAFKA],
    value_serializer=lambda dato: json.dumps(dato, ensure_ascii=False).encode("utf-8")
)

enviados = 0

try:
    with open(ARCHIVO, mode="r", encoding="utf-8-sig", newline="") as archivo:
        lector = csv.DictReader(archivo)

        for fila in lector:
            productor.send(TEMA, value=fila)
            enviados += 1

            if enviados % 1000 == 0:
                productor.flush()
                print(f"Registros enviados: {enviados}", flush=True)
                time.sleep(1)

    productor.flush()
    print(f"PRODUCTOR FINALIZADO. Total enviados: {enviados}", flush=True)

except KeyboardInterrupt:
    print(f"Productor detenido. Registros enviados: {enviados}", flush=True)

finally:
    productor.close()

