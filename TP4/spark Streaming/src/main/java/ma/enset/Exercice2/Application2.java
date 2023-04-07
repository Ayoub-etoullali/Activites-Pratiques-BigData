package ma.enset.Exercice2;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Application2 {
    public static void main(String[] args) throws InterruptedException {

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf conf=new SparkConf().setAppName("Exercice Vente with HDFS - Spark Streaming").setMaster("local[*]");

        JavaStreamingContext sc=new JavaStreamingContext(conf, Durations.seconds(10));

        JavaDStream<String> ventes=sc.textFileStream("hdfs://localhost:9000/ventes");

        JavaPairDStream<String, Double> ventesParVille = ventes.mapToPair(ligne -> new Tuple2<>(ligne.split(" ")[1], Double.parseDouble(ligne.split(" ")[3])));

        JavaPairDStream<String, Double> totalParVille = ventesParVille.reduceByKey(Double::sum);

        JavaPairDStream<String, Double> ventesParVilleParAnnee = ventes.mapToPair(ligne -> new Tuple2<>(ligne.split(" ")[0].split("/")[2] + " " + ligne.split(" ")[1], Double.parseDouble(ligne.split(" ")[3])));

        JavaPairDStream<String, Double> totalParVilleParAnnee = ventesParVilleParAnnee.reduceByKey(Double::sum);

        totalParVilleParAnnee.print();
        sc.start();
        sc.awaitTermination();

    }
}