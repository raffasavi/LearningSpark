from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, concat, lit, when
from pyspark.sql.types import DateType

# Crea una sessione Spark
spark = SparkSession.builder.appName("LeggiCSV").getOrCreate()

# Leggi il file CSV
df = spark.read.csv(r"C:\Users\raffa\Progetti Python\PROGETTO_1\accepted_2007_to_2018Q4.csv", header=True, inferSchema=True)

# Conta i valori nulli per ogni colonna
print("Conteggio dei valori nulli per ogni colonna:")
null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
null_counts.show()

# Conta i valori unici per ogni colonna
print("Conteggio dei valori unici per ogni colonna:")
unique_counts = {c: df.select(c).distinct().count() for c in df.columns}
for col_name, count in unique_counts.items():
    print(f"{col_name}: {count}")

# Lista delle colonne
cols = df.columns
print("Lista delle colonne:")
print(cols)

# Crea la colonna Real_MaturityDate
df = df.withColumn("Real_MaturityDate", concat(col("Maturity_Year"), lit("-"), col("Maturity_Month"), lit("-01")).cast(DateType()))
df = df.drop("Maturity_Year", "Maturity_Month", "Index")

# Crea la colonna Check_Early
df = df.withColumn("Check_Early", when(col("Real_MaturityDate") == col("Parsed_MaturityDate"), True).otherwise(False))
NestinzioniAnticipate = df.filter(col("Check_Early") == False).count()
print("Numero di estinzioni anticipate: ", NestinzioniAnticipate)

# Ottieni tutte le colonne di tipo stringa
categoricalCols = [item[0] for item in df.dtypes if item[1].startswith("string")]
print("Colonne categoriche:")
print(categoricalCols)

# Ottieni tutte le colonne di tipo intero
numericalCols = [item[0] for item in df.dtypes if item[1].startswith("int")]
print("Colonne numeriche:")
print(numericalCols)

# Ottieni tutte le colonne di tipo intero
dateCols = [item[0] for item in df.dtypes if item[1].startswith("date")]
print("Colonne con date:")
print(dateCols)

# Ottieni statistiche di base delle colonne numeriche
print("Statistiche di base delle colonne numeriche:")
print(df.select(numericalCols).describe().toPandas())

# Controlla la correlazione tra le colonne numeriche
print("Correlazione tra le colonne numeriche:")
numerical_data = df.select(numericalCols).toPandas()
print(numerical_data.corr())

# Ferma la sessione Spark
spark.stop()