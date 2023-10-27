#prac 5
from pyspark import SparkConf, SparkContext
import matplotlib.pyplot as plt

conf = SparkConf().setAppName("Word Count Example")
sc = SparkContext(conf=conf)

data = ["Redmi", "Samsung", "iphone", "Poco", "Motorola", "Samsung", "iphone", "Redmi", "Poco", "Poco"]
rdd = sc.parallelize(data)

# Word count
word_counts = rdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
print(word_counts.collect())

# Distinct operation
distinct_word = rdd.flatMap(lambda line: line.split(" ")).distinct()
print(distinct_word.collect())

# List characters
char_list = rdd.flatMap(lambda line: list(line))
char_count_results = char_list.countByValue()

# Separate characters and counts
characters, counts = zip(*char_count_results.items())

# Plot the character counts
plt.bar(characters, counts)
plt.xlabel("Characters")
plt.ylabel("Count")
plt.show()

# Stop the Spark context
sc.stop()
