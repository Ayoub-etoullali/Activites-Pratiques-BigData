from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
spark = SparkSession.builder.appName("tp pyspark").master("local[*]").getOrCreate()
dfLines=spark.readStream.format("socket").option("host","localhost").option("port",8888).load()
dfWords=dfLines.select(explode(split(dfLines["value"]," ")).alias("words"))
dfWordCount=dfWords.groupBy("words").count()
dfWordCount.writeStream.format("console").outputMode("update").trigger(processingTime='5 seconds').start().awaitTermination()