<<<<<<< HEAD
package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class Exercice3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Exercice 2").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("src\\main\\resources\\input\\2020.csv");

        // filter out header line
        JavaRDD<String> data = lines.filter(line -> !line.startsWith("ID"));

        // parse data and filter out non-temperature elements
//        JavaRDD<String[]> parsed = data.map(line -> line.split(","));
        JavaRDD<String> tempData = data.filter(fields -> fields.split(",")[2].equals("TMAX") || fields.split(",")[2].equals("TMIN"));

        // map data to key-value pairs of (station ID, temperature value)
        JavaPairRDD<String, Double> stationTemps = tempData.mapToPair(fields ->
                new Tuple2<>(fields.split(",")[0], Double.parseDouble(fields.split(",")[3]) / 10.0));

        // calculate statistics
        JavaPairRDD<String, Double> stationMinTemps = stationTemps.filter(kv -> kv._2() != null)
                .reduceByKey(Math::min);
        JavaPairRDD<String, Double> stationMaxTemps = stationTemps.filter(kv -> kv._2() != null)
                .reduceByKey(Math::max);
        JavaPairRDD<String, Double> stationAvgTemps = stationTemps.filter(kv -> kv._2() != null)
                .groupByKey().mapValues(vals -> {
                    double sum = 0.0;
                    int count = 0;
                    for (double val : vals) {
                        sum += val;
                        count++;
                    }
                    return sum / count;
                });

//        // find top 5 hottest and coldest stations
//        List<Tuple2<String, Double>> hottestStations = stationAvgTemps.takeOrdered(5);
////                (kv1, kv2) -> Double.compare(kv2._2(), kv1._2()));
//        List<Tuple2<String, Double>> coldestStations = stationAvgTemps.takeOrdered(5);
////                (kv1, kv2) -> Double.compare(kv1._2(), kv2._2()));

        Comparator<Double> s=Comparator.naturalOrder();

        // print results
        System.out.println("\n\n\nMinimum average temperature: " + stationAvgTemps.values().min(s));
        System.out.println("\n\n\nMaximum average temperature: " + stationAvgTemps.values().max(s));
        System.out.println("\n\n\nHighest maximum temperature: " + stationMaxTemps.values().max(s));
        System.out.println("\n\n\nLowest minimum temperature: " + stationMinTemps.values().min(s));
        System.out.println("\n\n\nTop 5 hottest stations:");
        System.out.println("\n\n\n");
//        for (Tuple2<String, Double> station : hottestStations) {
//            System.out.println(station._1() + ": " + station._2());
//        }
//        System.out.println("\n\n\n");
//        for (Tuple2<String, Double> station : coldestStations) {
//            System.out.println(station._1() + ": " + station._2());
//        }
        System.out.println("\n\n\n");
        // stop Spark
        sc.stop();
    }
=======
package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class Exercice3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Exercice 2").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("src\\main\\resources\\input\\1750.csv");

        // filter out header line
        JavaRDD<String> data = lines.filter(line -> !line.startsWith("ID"));

        // parse data and filter out non-temperature elements
//        JavaRDD<String[]> parsed = data.map(line -> line.split(","));
        JavaRDD<String> tempData = data.filter(fields -> fields.split(",")[2].equals("TMAX") || fields.split(",")[2].equals("TMIN"));

        // map data to key-value pairs of (station ID, temperature value)
        JavaPairRDD<String, Double> stationTemps = tempData.mapToPair(fields ->
                new Tuple2<>(fields.split(",")[0], Double.parseDouble(fields.split(",")[3]) / 10.0));

        // calculate statistics
        JavaPairRDD<String, Double> stationMinTemps = stationTemps.filter(kv -> kv._2() != null)
                .reduceByKey(Math::min);
        JavaPairRDD<String, Double> stationMaxTemps = stationTemps.filter(kv -> kv._2() != null)
                .reduceByKey(Math::max);
        JavaPairRDD<String, Double> stationAvgTemps = stationTemps.filter(kv -> kv._2() != null)
                .groupByKey().mapValues(vals -> {
                    double sum = 0.0;
                    int count = 0;
                    for (double val : vals) {
                        sum += val;
                        count++;
                    }
                    return sum / count;
                });

//        // find top 5 hottest and coldest stations
//        List<Tuple2<String, Double>> hottestStations = stationAvgTemps.takeOrdered(5);
////                (kv1, kv2) -> Double.compare(kv2._2(), kv1._2()));
//        List<Tuple2<String, Double>> coldestStations = stationAvgTemps.takeOrdered(5);
////                (kv1, kv2) -> Double.compare(kv1._2(), kv2._2()));

        Comparator<Double> s=Comparator.naturalOrder();

        // print results
        System.out.println("\n\n\nMinimum average temperature: " + stationAvgTemps.values().min(s));
        System.out.println("\n\n\nMaximum average temperature: " + stationAvgTemps.values().max(s));
        System.out.println("\n\n\nHighest maximum temperature: " + stationMaxTemps.values().max(s));
        System.out.println("\n\n\nLowest minimum temperature: " + stationMinTemps.values().min(s));
        System.out.println("\n\n\nTop 5 hottest stations:");
        System.out.println("\n\n\n");
//        for (Tuple2<String, Double> station : hottestStations) {
//            System.out.println(station._1() + ": " + station._2());
//        }
//        System.out.println("\n\n\n");
//        for (Tuple2<String, Double> station : coldestStations) {
//            System.out.println(station._1() + ": " + station._2());
//        }
        System.out.println("\n\n\n");
        // stop Spark
        sc.stop();
    }
>>>>>>> 4c60a0dc1d4a1c2ebb4a8a39850b32f80743e2f1
}