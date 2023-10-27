from pyspark.sql import SparkSession

# Initialize the Spark session
spark = SparkSession.builder.appName("Read CSV Example").getOrCreate()

# Read data from a CSV file
data_df = spark.read.csv(r"sample.csv", header=True, inferSchema=True)

# Display the DataFrame
data_df.show()

# Stop the Spark session (usually done at the end of your application)
spark.stop()
