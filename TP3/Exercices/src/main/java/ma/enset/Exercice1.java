package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Exercice1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Exercice 1").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("ayoub hayat samira radouan ayoub samira mustapha hayat karima samira hayat karima mustapha mustapha hayat samira radouan karima samira ayoub hayat samira mustapha hayat karima hayat radouan radouan hayat samira ihssan ihssan ElMehdi Aicha Aicha ElMehdi samira karima mustapha Aicha ElMehdi samira karima radouan ihssan ayoub hayat karima samira ayoub ihssan Aicha"));

        JavaRDD<String> rdd2 = rdd1.flatMap((word) -> Arrays.asList(word.split(" ")).iterator());

        JavaRDD<String> rdd3 = rdd2.filter(nom -> {
            if(!nom.equals("ihssan")){
                return true;
            }
            return false;
        });

        JavaRDD<String> rdd4 = rdd2.filter(nom -> {
            if(!nom.equals("ayoub")){
                return true;
            }
            return false;
        });

        JavaRDD<String> rdd5 = rdd2.filter(nom -> {
            if(!nom.equals("hayat")){
                return true;
            }
            return false;
        });

        JavaRDD<String> rdd6 = rdd3.union(rdd4);

        JavaRDD<String> rdd71 = rdd5.map(noms -> noms + " ETOULLALI");

        JavaRDD<String> rdd81 = rdd6.map(noms -> noms + " ETOULLALI");

        JavaPairRDD<String, Integer> rdd7 = rdd71.mapToPair((word) -> new Tuple2<>(word, 1));
        rdd7 = rdd7.reduceByKey((a, b) -> a + b);

        JavaPairRDD<String, Integer> rdd8 = rdd81.mapToPair((word) -> new Tuple2<>(word, 1));
        rdd8 = rdd8.reduceByKey((a, b) -> a + b);

        JavaPairRDD<String, Integer> rdd9 = rdd8.union(rdd7);

        JavaPairRDD<String, Integer> rdd10 = rdd9.sortByKey();

        //Action
        List<String> r1= rdd1.collect();
        //affichage
        System.out.println("\n\n######################## RDD 1 ############################\n\n");
        for (String n:r1) {
            System.out.println(n);
        }
        System.out.println("\n\n####################################################\n\n");

        //Action
        List<String> r2= rdd2.collect();
        //affichage
        System.out.println("\n\n####################### RDD 2 #############################\n\n");
        for (String n:r2) {
            System.out.println(n);
        }
        System.out.println("\n\n####################################################\n\n");

        //Action
        List<String> r3= rdd3.collect();
        //affichage
        System.out.println("\n\n###################### RDD 3 ##############################\n\n");
        for (String n:r3) {
            System.out.println(n);
        }
        System.out.println("\n\n####################################################\n\n");

        //Action
        List<String> r4= rdd4.collect();
        //affichage
        System.out.println("\n\n######################## RDD 4 ############################\n\n");
        for (String n:r4) {
            System.out.println(n);
        }
        System.out.println("\n\n####################################################\n\n");//Action

        //Action
        List<String> r5= rdd5.collect();
        //affichage
        System.out.println("\n\n######################## RDD 5 ############################\n\n");
        for (String n:r5) {
            System.out.println(n);
        }
        System.out.println("\n\n####################################################\n\n");

        //Action
        List<String> r6= rdd6.collect();
        //affichage
        System.out.println("\n\n####################### RDD 6 #############################\n\n");
        for (String n:r6) {
            System.out.println(n);
        }
        System.out.println("\n\n####################################################\n\n");

        //Action
        List<String> r71= rdd71.collect();
        //affichage
        System.out.println("\n\n######################## RDD 71 ############################\n\n");
        for (String n:r71) {
            System.out.println(n);
        }
        System.out.println("\n\n####################################################\n\n");

        //Action
        List<Tuple2<String,Integer>> r7= rdd7.collect();
        //affichage
        System.out.println("\n\n######################### RDD 7 ###########################\n\n");
        for (Tuple2<String,Integer> n:r7) {
            System.out.println(n);
        }
        System.out.println("\n\n####################################################\n\n");

        //Action
        List<String> r81= rdd81.collect();
        //affichage
        System.out.println("\n\n######################## RDD 81 ############################\n\n");
        for (String n:r81) {
            System.out.println(n);
        }
        System.out.println("\n\n####################################################\n\n");

        //Action
        List<Tuple2<String,Integer>> r8= rdd8.collect();
        //affichage
        System.out.println("\n\n########################### RDD 8 #########################\n\n");
        for (Tuple2<String,Integer> n:r8) {
            System.out.println(n);
        }
        System.out.println("\n\n####################################################\n\n");

        //Action
        List<Tuple2<String,Integer>> r9= rdd9.collect();
        //affichage
        System.out.println("\n\n######################### RDD 9 ###########################\n\n");
        for (Tuple2<String,Integer> n:r9) {
            System.out.println(n);
        }
        System.out.println("\n\n####################################################\n\n");

        //Action
        List<Tuple2<String,Integer>> r10= rdd10.collect();
        //affichage
        System.out.println("\n\n########################### RDD 10 #########################\n\n");
        for (Tuple2<String,Integer> n:r10) {
            System.out.println(n);
        }
        System.out.println("\n\n####################################################\n\n");


//        JavaRDD<Double> rdd2=rdd1.map((a)->a+1);
//        JavaRDD<Double> rdd3=rdd2.filter((a)->{
//            if (a>10)return true;
//            return false;
//        });
//
//       List<Double> notes= rdd3.collect();
//        for (double n:notes) {
//            System.out.println(n);
//        }
    }
}
