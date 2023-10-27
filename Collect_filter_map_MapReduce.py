from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("Odd_Even").getOrCreate()

data = [1, 2, 3, 4, 5, 6, 7, 8, 9]

# Create an RDD
rdd = spark.sparkContext.parallelize(data)

# Collect the data
collected_data = rdd.collect()
print("Collect Function:", collected_data)

# Find even and odd numbers using filter()
filter_rdd1 = rdd.filter(lambda x: x % 2 == 0)
filter_rdd2 = rdd.filter(lambda x: x % 2 != 0)
filter_data1 = filter_rdd1.collect()
filter_data2 = filter_rdd2.collect()
print("Even numbers using filter():", filter_data1)
print("Odd numbers using filter():", filter_data2)

# Map the data
square_rdd1 = rdd.map(lambda x: x * x)
filter_data3 = square_rdd1.collect()
print("Squaring using map():", filter_data3)

# Map and then reduce
square_rdd2 = rdd.map(lambda x: x * x).reduce(lambda a, b: a + b)
print("Squaring and then adding using mapreduce():", square_rdd2)

# Reduce the data
square_rdd3 = rdd.reduce(lambda a, b: a + b)
print("Addition of data using reduce():", square_rdd3)

# Stop the Spark session
spark.stop()
