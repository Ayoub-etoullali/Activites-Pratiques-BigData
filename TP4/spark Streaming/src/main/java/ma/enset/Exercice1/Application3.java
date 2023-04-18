package ma.enset.Exercice1;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Application3 {
    public static void main(String[] args) throws InterruptedException {

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf conf=new SparkConf().setAppName("Word Count with TCP - Spark Streaming").setMaster("local[*]");

        JavaStreamingContext sc=new JavaStreamingContext(conf, new Duration(10000));

        // Create a DStream that will connect to hostname:port, like localhost:9090
        JavaReceiverInputDStream<String> DStreamLines = sc.socketTextStream("localhost", 9090); //pas démarrer un serveur mais connecter à un serveur

        JavaDStream<String> DStreamWords = DStreamLines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // Count each word in each batch
        JavaPairDStream<String, Integer> DStreamWordsPair = DStreamWords.mapToPair(mot -> new Tuple2<>(mot, 1));
        JavaPairDStream<String, Integer> DStreamWordsPairCount = DStreamWordsPair.reduceByKey(Integer::sum);

        DStreamWordsPairCount.print();
        sc.start();
        sc.awaitTermination();

    }
}