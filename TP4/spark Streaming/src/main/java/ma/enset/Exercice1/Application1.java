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

        Logger.getLogger("org").setLevel(Level.OFF);

        /*
        Logger.getLogger("org").setLevel(Level.ERROR);

        Logger.getLogger("org").setLevel(Level.ERROR);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.ERROR);

        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.spark-project").setLevel(Level.WARN);
         */

        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf=new SparkConf().setAppName("Word Count with file - Spark Streaming").setMaster("local[*]");

        JavaStreamingContext sc=new JavaStreamingContext(conf, new Duration(10000));

//        JavaDStream<String> DStreamLines=sc.textFileStream("D:\\Leçons\\ENSET Mohammedia\\ENSET\\S4\\Big data\\TPs\\Activites-Pratiques-BigData\\TP4\\spark Streaming\\src\\main\\resources\\input\\names");
        JavaDStream<String> DStreamLines=sc.textFileStream("src/main/resources/input/names");

        // Split each line into words
        JavaDStream<String> DStreamWords = DStreamLines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairDStream<String, Integer> DStreamWordsPair = DStreamWords.mapToPair(mot -> new Tuple2<>(mot, 1));

        JavaPairDStream<String, Integer> DStreamWordsPairCount = DStreamWordsPair.reduceByKey((a, b) -> a + b);

        DStreamWordsPairCount.print(); // Print the first ten elements of each RDD generated in this DStream to the console
        sc.start(); // Start the computation
        sc.awaitTermination(); // Wait for the computation to terminate

    }
}