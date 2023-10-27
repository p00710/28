# Prac 4
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


#create a spark configuration
spark_conf=SparkConf()
spark_conf.set("spark.app.name","DataFrame")
spark_conf.set("spark.executor.memory","2g")
spark_conf.set("spark.cores.max","2")


#create a spark session using the configuration
spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()


#sample data
data=[("Umang",23),("Saurabh",21),("Heeba",22),("Dhwani",20),("Rahul",23),("Zaid",20)]


columns="Name","Age"


#Create a dataframe from the sample data
df=spark.createDataFrame(data,columns)
df.show()


# Stop the Spark session (usually done at the end of your application)
spark.stop()
