package ma.enset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import scala.collection.Seq;

import static org.apache.spark.sql.functions.avg;
import  static org.apache.spark.sql.functions.col;

public class Application1 {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkSession ss=SparkSession.builder().master("local[*]").appName("tp").getOrCreate();

//    Dataset<Row> dfEmpl=ss.read().json("employees.json"); //read.format("json","path")

        Dataset<Row> dfEmpl=ss.read().option("multiline",true).json("D:\\Leçons\\ENSET Mohammedia\\ENSET\\S4\\Big data\\Tests\\spark-sql\\spark-sql\\src\\main\\resources\\employees.json");

        dfEmpl.printSchema();

        dfEmpl.show();

        dfEmpl.select("name","salary").show();
        dfEmpl.select("department", "salary")
                .groupBy("department")
                .avg("salary")
                .show();



    }


}