package ma.enset.Exercice1;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class Application1 {
    public static void main(String[] args) throws InterruptedException {


        SparkConf conf=new SparkConf().setAppName("Word Count with socket - Spark Streaming").setMaster("local[*]");

        JavaStreamingContext sc=new JavaStreamingContext(conf, new Duration(1000));

        JavaDStream<String> DStreamLines=sc.textFileStream("names");

        Logger.getLogger("org").setLevel(Level.OFF);
        /*
        Logger.getLogger("org").setLevel(Level.ERROR);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.ERROR);

        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.spark-project").setLevel(Level.WARN);
         */

        JavaDStream<String> DStreamWords = DStreamLines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairDStream<String, Integer> DStreamWordsPair = DStreamWords.mapToPair(mot -> new Tuple2<>(mot, 1));

        JavaPairDStream<String, Integer> DStreamWordsPairCount = DStreamWordsPair.reduceByKey((a, b) -> a + b);

        DStreamWordsPairCount.print();
        sc.start();
        sc.awaitTermination();

    }
}