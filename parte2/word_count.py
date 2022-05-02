from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql.types import *
import sys

spark = SparkSession.builder.appName("StructuredNetworkWordCount").config("spark.scheduler.mode", "FAIR").config("spark.streaming.concurrentJobs","10").getOrCreate()
#spark = SparkSession.builder.appName("StructuredNetworkWordCount").config("spark.streaming.concurrentJobs","10").getOrCreate()

lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "topico").option("port", 9999).load()
df = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

wordCounts = words.groupBy("word").count()
allWords = words.groupBy().count()
pWords = words.filter(F.upper(F.col("word").substr(1, 1)) == "P").groupBy().count()
pWords = pWords.selectExpr("cast (count as string) value")

sWords = words.filter(F.upper(F.col("word").substr(1, 1)) == "S").groupBy().count()
sWords = sWords.selectExpr("cast (count as string) value")

rWords = words.filter(F.upper(F.col("word").substr(1, 1)) == "R").groupBy().count()
rWords = rWords.selectExpr("cast (count as string) value")

words6 = words.filter(F.length("word") == 6).groupBy().count()
words6 = words6.selectExpr("cast (count as string) value")

words8 = words.filter(F.length("word") == 8).groupBy().count()
words8 = words8.selectExpr("cast (count as string) value")

words11 = words.filter(F.length("word") == 11).groupBy().count()
words11 = words11.selectExpr("cast (count as string) value")


query = wordCounts.writeStream.outputMode("complete").format("console").trigger(processingTime='7 second').option('numRows', 20).start()
query2 = allWords.writeStream.outputMode("complete").format("console").trigger(processingTime='7 second').option('numRows', 20).start()

query3 = pWords.writeStream.outputMode("update").format("kafka").trigger(processingTime='5 second').option("kafka.bootstrap.servers", "localhost:9092").option("topic", "PalavrasComP").option("checkpointLocation", "./checkpoints").start()
query4 = sWords.writeStream.outputMode("update").format("kafka").trigger(processingTime='5 second').option("kafka.bootstrap.servers", "localhost:9092").option("topic", "PalavrasComS").option("checkpointLocation", "./checkpoints2").start()
query5 = rWords.writeStream.outputMode("update").format("kafka").trigger(processingTime='5 second').option("kafka.bootstrap.servers", "localhost:9092").option("topic", "PalavrasComR").option("checkpointLocation", "./checkpoints3").start()

query5 = words6.writeStream.outputMode("update").format("kafka").trigger(processingTime='5 second').option("kafka.bootstrap.servers", "localhost:9092").option("topic", "PalavrasCom6").option("checkpointLocation", "./checkpoints4").start()
query6 = words8.writeStream.outputMode("update").format("kafka").trigger(processingTime='5 second').option("kafka.bootstrap.servers", "localhost:9092").option("topic", "PalavrasCom8").option("checkpointLocation", "./checkpoints5").start()
query7 = words11.writeStream.outputMode("update").format("kafka").trigger(processingTime='5 second').option("kafka.bootstrap.servers", "localhost:9092").option("topic", "PalavrasCom11").option("checkpointLocation", "./checkpoints6").start()

spark.streams.awaitAnyTermination()
