package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
public class Application1 {
    public static void main(String[] args) throws AnalysisException {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss=SparkSession.builder().master("local[*]").appName("tp spark sql").getOrCreate();
        //json
        Dataset<Row> dfEmpl = ss.read().option("multiline",true).json("employes.json");
        dfEmpl.printSchema();
        //dfEmpl.select("name","salary").show();
       // dfEmpl.select(col("name"),col("salary").plus(2000).alias("salary")).show();
       dfEmpl.filter(col("salary").gt(25000).and(col("name").startsWith("K"))).show();
      // dfEmpl.filter("salary>25000 and name like 'K%'").show();
        //dfEmpl.createTempView("employes");
        //ss.sql("select id,name,age,salary from employes where salary>20000").show();
        //afficher le salaire moyen et max de chaque departement
        dfEmpl.select("departement","salary").groupBy("departement").avg("salary").show();

    }
}