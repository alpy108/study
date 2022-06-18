#from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *  

spark = SparkSession.builder.appName("Spark HomeWorks").master("local[2]").getOrCreate()


## -----------открытие файла--------------------------------


df = spark.read.option("header", "true").option("inferSchema", "true").csv("/opt/bitnami/spark/owid-covid-data.csv")

print ("==================================================")
print ("==================================================")

print ("Задание 1: Выберите 15 стран с наибольшим процентом переболевших на 31 марта (в выходящем датасете необходимы колонки: iso_code, страна, процент переболевших)")

print ("Задание 2: Top 10 стран с максимальным зафиксированным кол-вом новых случаев за последнюю неделю марта 2021 в отсортированном порядке по убыванию (в выходящем датасете необходимы колонки: число, страна, кол-во новых случаев)")

print ("Задание 3: Посчитайте изменение случаев относительно предыдущего дня в России за последнюю неделю марта 2021. (например: в россии вчера было 9150, сегодня 8763, итог: -387) (в выходящем датасете необходимы колонки: число, кол-во новых случаев вчера, кол-во новых случаев сегодня, дельта)")

print ("==================================================")
print ("==================================================")

job_1 = df.select("iso_code","location","date","reproduction_rate").sort(col("reproduction_rate").desc()).where(col("date") == "2020-03-31").limit(15)

job_2 = df.select("date","location","new_cases").sort(col("new_cases").desc()).where((col("date") <= "2021-03-31") & (col("date") > "2021-03-24")).limit(10)

w = Window.partitionBy("location").orderBy("date")
job_3 = df.select("date", lag(col("new_cases"), 1).over(w).alias("lag_new_cases"), "new_cases", (col("new_cases")-lag(col("new_cases"),1).over(w)).alias("delta")).where((col("date").between("2021-03-24", "2021-03-31")) & (col("iso_code") == "RUS"))

print ("==================================================")

job_1.show()

print ("==================================================")

job_2.show()

print ("==================================================")

job_3.show()

print ("==================================================")

job_1.write.json("outfolder/job1")
job_2.write.json("outfolder/job2")
job_3.write.json("outfolder/job3")

spark.stop()