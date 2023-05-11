# TP6: Ventes avec Structured Streaming en pyhton

## Exercice 1 : Word Count

```
>> from pyspark.sql import SparkSession
>> from pyspark.sql.functions import explode
>> from pyspark.sql.functions import split
>> spark = SparkSession.builder.appName("tp pyspark").master("local[*]").getOrCreate()
>> dfLines=spark.readStream.format("socket").option("host","localhost").option("port",8888).load()
>> dfWords=dfLines.select(explode(split(dfLines["value"]," ")).alias("words"))
>> dfWordCount=dfWords.groupBy("words").count()
>> dfWordCount.writeStream.format("console").outputMode("update").trigger(processingTime='5 seconds').start().awaitForTermination()
```

![image](https://github.com/Ayoub-etoullali/Activites-Pratiques-BigData/assets/92756846/f00136f0-e111-47cc-821d-8f379803d8a7)

![image](https://github.com/Ayoub-etoullali/Activites-Pratiques-BigData/assets/92756846/51364a03-4091-4b8a-a2a1-488aacfc2c5d)

## Exercice 2 :
![image](https://user-images.githubusercontent.com/92756846/224802856-e9fefc64-4178-4037-b94b-8b48dfdc1439.png)
  
  #### Fichier "ventes.txt"
  ![image](https://user-images.githubusercontent.com/92756846/225772439-ea4eb6c8-1472-40a0-b109-bf214532374b.png)

  ### Question 1 :
```
>> from pyspark.sql import SparkSession
>> spark = SparkSession.builder.appName("tp pyspark").master("local[*]").getOrCreate()
>> df=spark.readStream.format("socket").option("host","localhost").option("port","8088").load()
>> ventesParVill=ventes.mapToPair(ligne -> new Tuple2<>(ligne.split(" ")[1], Double.parseDouble(ligne.split(" ")[3])))
>> totalParVille = ventesParVille.reduceByKey(Lambda v1,v2:v1+v2)
>> totalParVille.WriteStream.format("console").output("append").trigger(processingTime="6 seconds").start().awaitForTermination()
```
  ### Question 2 : 
```
>> from pyspark.sql import SparkSession
>> spark = SparkSession.builder.appName("tp pyspark").master("local[*]").getOrCreate()
>> df=spark.readStream.format("socket").option("host","localhost").option("port","8088").load()
>> ventesParVilleParAnnee = ventes.mapToPair(ligne -> new Tuple2<>(ligne.split(" ")[0].split("/")[2] + " " + ligne.split(" ")[1], Double.parseDouble(ligne.split(" ")[3])));
>> totalParVilleParAnnee = ventesParVilleParAnnee.reduceByKey(Lambda v1,v2:v1+v2)
>> totalParVilleParAnnee.WriteStream.format("console").output("append").trigger(processingTime="6 seconds").start().awaitForTermination()
```
#### Demo :
<div align="center">
       <p>
       <sup>  <strong>Vidéo -</strong> Ventes avec Structured Streaming en pyhton</sup>
       </p>
</div>

<kbd>Enjoy Code</kbd> 👨‍💻
