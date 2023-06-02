package ma.enset.Exercice1;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
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

        SparkConf conf = new SparkConf().setAppName("Word Count with HDFS - Spark Streaming").setMaster("local[*]");

        JavaStreamingContext sc = new JavaStreamingContext(conf, new Duration(10000));

        JavaDStream<String> DStreamLines = sc.textFileStream("hdfs://localhost:19000/ventes");

        JavaDStream<String> DStreamWords = DStreamLines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairDStream<String, Integer> DStreamWordsPair = DStreamWords.mapToPair(mot -> new Tuple2<>(mot, 1));

        JavaPairDStream<String,
                Integer> DStreamWordsPairCount = DStreamWordsPair.reduceByKey((a, b) -> a + b);

        DStreamWordsPairCount.print();
        sc.start();
        sc.awaitTermination();
    }
}