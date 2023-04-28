package ma.enset.dataMysql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;


import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.countDistinct;

public class Dataframes {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkSession ss=SparkSession
                .builder()
                .master("local[*]")
                .appName("Traitement de données stockées dans Mysql [Dataset,Dataframe]")
                .getOrCreate();

        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/DB_HOPITAL");
        options.put("user","root");
        options.put("password","");

        System.out.println("\n----------------Afficher le nombre de consultations par jour----------------\n");
        Dataset<Row> df1 = ss.read().format("jdbc")
                .options(options)
                .option("dbtable","CONSULTATIONS")
                .load();
        df1.select("DATE_CONSULTATION").groupBy("DATE_CONSULTATION").count().show();
//        df1.select("DATE_CONSULTATION").as("Date de consultaion").groupBy("DATE_CONSULTATION").count().as("Nombre").show();

        System.out.println("\n\n----------------Afficher le nombre de consultation par médecin----------------\n");
        Dataset<Row> consultations = ss.read().format("jdbc").options(options).option("dbtable","CONSULTATIONS").load();
        Dataset<Row> medecins = ss.read().format("jdbc").options(options).option("dbtable","MEDECINS").load();
        consultations
                .join(medecins, consultations.col("ID_MEDECIN").equalTo(medecins.col("ID")))
                .groupBy(medecins.col("NOM"), medecins.col("PRENOM"))
                .count().as("NB_CONSULTATIONS")
                .show();

        System.out.println("\n\n----------------Afficher pour chaque médecin, le nombre de patients qu’il a assisté----------------\n");
        consultations
                .join(medecins, consultations.col("ID_MEDECIN").equalTo(medecins.col("ID")))
                .groupBy(medecins.col("NOM"), medecins.col("PRENOM"))
                .agg(countDistinct(consultations.col("ID_PATIENT")).as("NB_PATIENTS")).show();

    }
}
