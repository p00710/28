# Import the necessary PySpark modules
from pyspark.sql import SparkSession

# Create a Spark session with the name "Tuple_data"
spark = SparkSession.builder.appName("Tuple_data").getOrCreate()

# Define the data for the first DataFrame
data1 = [("Umang", "1"), ("Saurabh", "2"), ("Heeba", "3")]

# Define the data for the second DataFrame
data2 = [("Umang", "Engineer"), ("Saurabh", "Doctor"), ("Rahul", "Student")]

# Define the column names for the first DataFrame
col1 = ["Name", "RollNo"]

# Define the column names for the second DataFrame
col2 = ["Name", "Profession"]

# Create the first DataFrame (df1) using the data and column names
# Convert the "RollNo" column to integer
df1 = spark.createDataFrame(data1, col1)
df1 = df1.withColumn("RollNo", df1["RollNo"].cast("int"))

# Create the second DataFrame (df2) using the data and column names
df2 = spark.createDataFrame(data2, col2)

# Perform an inner join on df1 and df2 using the "Name" column as the join key and store the result in join_df
join_df = df1.join(df2, "Name", "inner")

# Display the result of the inner join
print("Inner Function")
join_df.show()

# Stop the Spark session to release resources
spark.stop()
