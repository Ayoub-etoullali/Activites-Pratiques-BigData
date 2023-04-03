import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Application2 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf();
        conf.setAppName("TP word count").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> lines=sc.textFile("names.txt");
        JavaRDD<String> words=lines.flatMap((line)-> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String,Integer> wordPairs=words.mapToPair((word)->new Tuple2<>(word,1));
        JavaPairRDD<String,Integer> wordPairsCount=wordPairs.reduceByKey((a,b)->a+b);
        List<Tuple2<String,Integer>>  wordCount=wordPairsCount.collect();
        for (Tuple2<String,Integer> word:wordCount) {
            System.out.println(word._1()+" "+word._2());
        }
    }
}
