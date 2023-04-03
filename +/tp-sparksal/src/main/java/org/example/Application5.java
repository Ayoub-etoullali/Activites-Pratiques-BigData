package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

public class Application5 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss=SparkSession.builder().master("local[*]").appName("tp spark sql").getOrCreate();
        Encoder<Employe> employeEncoder= Encoders.bean(Employe.class);
        Dataset<Row> dfEmp = ss.read().option("multiline", true).json("employes.json");
        //dataFrame vers DataSet
        Dataset<Employe> dsEmp = dfEmp.as(employeEncoder);
        Dataset<Employe> dsEmp2 = dfEmp.map((MapFunction<Row, Employe>) row -> {
            Employe employe = new Employe();
            employe.setAge(row.getLong(0));
            employe.setDepartement(row.getString(1));
            employe.setId(row.getLong(2));
            employe.setName(row.getString(3));
            employe.setSalary(row.getDouble(4));
            return employe;
        }, employeEncoder);
        dsEmp2.show();
        //dataset vers RDD
        JavaRDD<Employe> employeRDD = dsEmp.javaRDD();
        //dataFrame vers RDD
        JavaRDD<Employe> employeRDD1=dfEmp.as(employeEncoder).javaRDD();
        //RDD vers DataFrame
        Dataset<Row> dfEmp1=ss.createDataFrame(employeRDD1,Employe.class);
        //dfEmp1.show();
        //RDD vers DataSet
        Dataset<Employe> dsEmp1 = ss.createDataset(employeRDD1.rdd(), employeEncoder);
       //dsEmp1.show();
       //DataSet vers DataFrame

    }
}
