package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Function1;

public class Application4 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss=SparkSession.builder().master("local[*]").appName("tp spark sql").getOrCreate();
        Encoder<Employe> employeEncoder= Encoders.bean(Employe.class);
        Dataset<Employe> dsEmp = ss.read().option("multiline", true).json("employes.json").as(employeEncoder);
        //dsEmp.printSchema();
        //dsEmp.show();
        dsEmp.filter((FilterFunction<Employe>) employe->employe.getAge()>30).show();
        dsEmp.filter("age>30").show();

    }
}
