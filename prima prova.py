from pyspark.sql import SparkSession

# Crea una sessione Spark
spark = SparkSession.builder.appName("LeggiCSV").getOrCreate()

# Leggi il file CSV
df = spark.read.csv("path/to/your/file.csv", header=True, inferSchema=True)

# Mostra il contenuto del DataFrame
df.show()

# Ferma la sessione Spark
spark.stop()