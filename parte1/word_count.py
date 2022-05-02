from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql import Row
from pyspark.sql.types import *
import sys

total_de_palavras = 0

def func(batch_df, batch_id):
	sys.stdout.write("TESTEEEEEEEEEEEEE")
	pass

spark = SparkSession.builder.appName("StructuredNetworkWordCount").config("spark.scheduler.mode", "FAIR").config("spark.streaming.concurrentJobs","3").getOrCreate()

lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()


words = lines.select(
explode(
split(lines.value, " ")
).alias("word")
)

wordCounts = words.groupBy("word").count()
query = wordCounts.writeStream.outputMode("complete").foreachBatch(func).format("console").option('numRows', 1000).start()

spark.streams.awaitAnyTermination()
