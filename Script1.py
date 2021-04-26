import findspark
findspark.init('/home/leonardo/spark-3.1.1-bin-hadoop2.7')
from pyspark.sql import SparkSession
# Crear sesion de Spark
spark = SparkSession \
    .builder \
    .appName("UNIVERSO INTERACCION") \
    .getOrCreate()

from pyspark.sql import functions as F
from pyspark.sql.functions import col

# Crear Spark Dataframes de archivos .parquet
m_destipointeraccion = spark.read.parquet("./inputs caso practico/m_destipointeraccion");
h_interaccioncajero = spark.read.parquet("./inputs caso practico/h_interaccioncajero");

# PASO 1
# Crear UNIVERSO INTERACCIONES VALIDAS
columns1 = ["tipinteraccion", "mtointeraccionsol"]
TP_UNIV_INTERACCION = h_interaccioncajero.where("flgvalida = 'S'").select(*columns1)

# PASO 2
# Crear UNIVERSO DE MAESTRA TIPO INTERACCIONES VALIDAS para codcanal 1002
columns2 = ["tipinteraccion", "destipinteraccion",]
TP_UNIV_TIPO_INTERACCION = h_interaccioncajero.where("codcanal = '1002'").select(*columns2)

# PASO 3
# Cruce de tablas temporales por tipinteraccion
TP_UNIVERSOS_INTERACCION = TP_UNIV_INTERACCION.alias("a") \
                           .join(TP_UNIV_TIPO_INTERACCION.alias("b"), \
                           TP_UNIV_INTERACCION.tipinteraccion == TP_UNIV_TIPO_INTERACCION.tipinteraccion, \
                           how='left').select("a.tipinteraccion", "a.mtointeraccionsol", "b.destipinteraccion")
# PASO 4
# Agrupacion, creacion de nuevos registros y tabla maestra final
SUMMARY_INTERACTIONS = TP_UNIVERSOS_INTERACCION.groupBy("tipinteraccion", "destipinteraccion") \
                          .agg(F.count(col("tipinteraccion")), F.sum(col("mtointeraccionsol"))) \
                          .withColumnRenamed("tipinteraccion", "INTERACTION_CODE") \
                          .withColumnRenamed("destipinteraccion", "INTERACTION_DESCRIPTION") \
                          .withColumnRenamed("count(tipinteraccion)", "INTERACTION_NUMBER") \
                          .withColumnRenamed("sum(mtointeraccionsol)", "INTERACTION_AMOUNT")

#Crear vista 
SUMMARY_INTERACTIONS.createOrReplaceTempView("SUMMARY_INTERACTIONS")
# Mostrar tabla final 
spark.sql("SELECT * FROM SUMMARY_INTERACTIONS").show()

# Parar la sesion de spark al finalizar la ejecucion
spark.stop()
