package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Exercice2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Exercice 2").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> ventes = sc.textFile("src\\main\\resources\\input\\ventes.txt");

        //_____Transformation______
        JavaPairRDD<String, Double> ventesParVille = ventes.mapToPair(
                ligne -> new Tuple2<>(ligne.split(" ")[1], Double.parseDouble(ligne.split(" ")[3])));
        //Action
        List<Tuple2<String, Double>> action1 = ventesParVille.collect();
        System.out.println("\n\n######################## ventesParVille ############################\n\n");
        for (Tuple2<String, Double> n:action1) {
            System.out.println(n);
        }
        System.out.println("\n\n####################################################\n\n");

        //_____Transformation______
        JavaPairRDD<String, Double> totalParVille = ventesParVille.reduceByKey(
                Double::sum);
        //Action
        List<Tuple2<String, Double>> action2 = totalParVille.collect();
        System.out.println("\n\n######################## totalParVille ############################\n\n");
        for (Tuple2<String, Double> n:action2) {
            System.out.println(n);
        }
        System.out.println("\n\n####################################################\n\n");

        //_____Transformation______
        JavaPairRDD<String, Double> ventesParVilleParAnnee = ventes.mapToPair(
                ligne -> new Tuple2<>(ligne.split(" ")[0].split("/")[2]+" "+ligne.split(" ")[1], Double.parseDouble(ligne.split(" ")[3])));
        //Action
        List<Tuple2<String, Double>> action3 = ventesParVilleParAnnee.collect();
        System.out.println("\n\n######################## ventesParVilleParAnnee ############################\n\n");
        for (Tuple2<String, Double> n:action3) {
            System.out.println(n);
        }
        System.out.println("\n\n####################################################\n\n");

        //_____Transformation______
        JavaPairRDD<String, Double> totalParVilleParAnnee = ventesParVilleParAnnee.reduceByKey(
                Double::sum);
        //Action
        List<Tuple2<String, Double>> action4 = totalParVilleParAnnee.collect();
        System.out.println("\n\n######################## totalParVilleParAnnee ############################\n\n");
        for (Tuple2<String, Double> n:action4) {
            System.out.println(n);
        }
        System.out.println("\n\n####################################################\n\n");
    }
}
