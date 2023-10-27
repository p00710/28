from pyspark import SparkContext
from pyspark.streaming import StreamingContext


#create a sparkcontext and streaming context
sc=SparkContext("local[2]","pysparkstreamingwordreduce")
ssc=StreamingContext(sc,1)


#Create a socket stream to listen for data on local host
lines=ssc.socketTextStream("localhost",9999)
ws=lines.window(10,2) #we defining a window for 10secs and sliding interval 2 secs
words_counts=ws.flatMap(lambda line:line.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda x ,y:x+y)
words_counts.pprint()


#start streaming
ssc.start()
ssc.awaitTermination()
