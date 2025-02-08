from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Crea una sessione Spark
spark = SparkSession.builder.appName("LeggiCSV").getOrCreate()

# Leggi il file CSV
df = spark.read.csv(r"C:\Users\raffa\Progetti Python\PROGETTO_1\Preprocessed_Data_Improved.csv", header=True, inferSchema=True)

# Mostra le prime 5 righe del DataFrame
print("Prime 5 righe del DataFrame:")
df.show(5)

# Mostra lo schema del DataFrame
print("Schema del DataFrame:")
df.printSchema()

# Calcola statistiche descrittive
print("Statistiche descrittive del DataFrame:")
df.describe().show()

# Conta i valori nulli per ogni colonna
print("Conteggio dei valori nulli per ogni colonna:")
null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
null_counts.show()

# Conta i valori unici per ogni colonna
print("Conteggio dei valori unici per ogni colonna:")
unique_counts = {c: df.select(c).distinct().count() for c in df.columns}
for col_name, count in unique_counts.items():
    print(f"{col_name}: {count}")

# Ferma la sessione Spark
spark.stop()