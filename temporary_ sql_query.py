from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# create a spark session
spark = SparkSession.builder.appName("Example1").getOrCreate()


# read the ccsv file
df = spark.read.csv("prac7.csv", header=True, inferSchema=True)
df.printSchema()


# create temporary view of dataframe to use sql query
df.createOrReplaceTempView("employee")  # temp table -> employee
result = spark.sql("select * from employee where Age > 25 or Gender='Female'")
result1 = spark.sql("select * from employee where Age > 20 and Gender='Male'")
result.show()
result1.show()
spark.stop()
