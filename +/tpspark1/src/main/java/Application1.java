import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Application1 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf();
        conf.setAppName("TP 1 Spark").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<Double> rdd1=sc.parallelize(Arrays.asList(12.5,10.0,17.5,9.5));
        JavaRDD<Double> rdd2=rdd1.map((a)->a+1);
        JavaRDD<Double> rdd3=rdd2.filter((a)->{
            if (a>10)return true;
            return false;
        });

       List<Double> notes= rdd3.collect();
        for (double n:notes) {
            System.out.println(n);
        }
    }
}
