import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;

public class Application3 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf=new SparkConf().setAppName("App").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Double> rdd1=sc.parallelize(Arrays.asList(12.0,10.0,15.5,9.5,10.0));
        JavaRDD<Double> rdd2=rdd1.map(aDouble ->{
            System.out.println("**** map ****");
            return  aDouble+1;
        });

        rdd2.persist(StorageLevel.MEMORY_ONLY());
        
        JavaRDD<Double> rdd3=rdd2.filter(aDouble -> {
            System.out.println("**** filter ****");
            return aDouble>=10;
        });


        List<Double> vals= rdd3.collect();
        for (double a:vals) {
            System.out.println(a);
        }


        List<Double> vals1= rdd3.collect();
        for (double a:vals1) {
            System.out.println(a);
        }
    }
}
