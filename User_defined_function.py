from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Create a Spark session
spark = SparkSession.builder.appName("GreetingExample").getOrCreate()

# Read the CSV file
df = spark.read.csv("name.csv", header=True, inferSchema=True)

# Define a function to greet by name
def greet(name):
    return f"Hello, {name}"

# Register the greeting function as a UDF (User-Defined Function)
greet_udf = udf(greet, StringType())

# Add a new column "Greeting" with the greeting message
df_greeting = df.withColumn("Greeting", greet_udf(df["Name"]))

# Show the DataFrame with greetings
df_greeting.show()

# Stop the Spark session
spark.stop()
