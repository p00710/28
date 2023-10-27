from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
import matplotlib.pyplot as plt
import pandas as pd
from sklearn import datasets

# Create a Spark session
spark = SparkSession.builder.appName("KMeansIris").getOrCreate()

# Load the Iris dataset from scikit-learn
iris = datasets.load_iris()
data = iris.data  # Features
feature_names = iris.feature_names

# Create a Pandas DataFrame from the Iris data
iris_df = pd.DataFrame(data, columns=feature_names)

# Create a PySpark DataFrame from the Pandas DataFrame
iris_data = spark.createDataFrame(iris_df)

# Select relevant features (attributes) for clustering
feature_columns = ["sepal length (cm)", "sepal width (cm)", "petal length (cm)", "petal width (cm)"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data = assembler.transform(iris_data)

# Train a K-Means clustering model
kmeans = KMeans().setK(3).setSeed(1)  # Set the number of clusters (K) to 3
model = kmeans.fit(data)

# Get cluster centers
cluster_centers = model.clusterCenters()
print("Cluster Centers:")
for center in cluster_centers:
    print(center)

# Assign data points to clusters
predictions = model.transform(data)

# Convert the PySpark DataFrame to a Pandas DataFrame for visualization
df = predictions.select("features", "prediction").toPandas()

# Plot the clustered data points
plt.scatter(df["features"].apply(lambda x: x[0]), df["features"].apply(lambda x: x[1]), c=df["prediction"], cmap="rainbow")
plt.xlabel("Sepal Length (cm)")
plt.ylabel("Sepal Width (cm)")
plt.title("K-Means Clustering of Iris Dataset")
plt.show()

# Stop the Spark session
spark.stop()
