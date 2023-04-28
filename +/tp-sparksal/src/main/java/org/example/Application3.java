package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.columnar.MAP;

import java.util.HashMap;
import java.util.Map;

public class Application3 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss=SparkSession.builder().master("local[*]").appName("tp spark sql").getOrCreate();
        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/DB_Spark");
        options.put("user","root");
        options.put("password","");

        Dataset<Row> dfEmp = ss.read().format("jdbc")
                .options(options)
                //.option("dbtable", "EMPLOYES")
                .option("query","select ID,NAME,SALARY from EMPLOYES")
                .load();
        dfEmp.printSchema();
        dfEmp.show();


    }
}
