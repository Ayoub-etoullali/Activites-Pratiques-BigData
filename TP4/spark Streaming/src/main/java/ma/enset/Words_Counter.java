package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Words_Counter {
    public static void main(String[] args) throws InterruptedException {

        Logger.getLogger("org").setLevel(Level.OFF);
        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(5));
        // Create a DStream that will connect to hostname:port, like localhost:9999
//        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9090);

        JavaDStream<String> lines=sc.textFileStream("src\\main\\resources\\input\\names");

        // Split each line into words
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
// Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

// Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();

        sc.start();              // Start the computation
        sc.awaitTermination();   // Wait for the computation to terminate


    }
}