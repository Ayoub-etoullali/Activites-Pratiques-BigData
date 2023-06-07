package ma.enset.dataMysql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

public class Dataframes {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkSession spark = SparkSession.builder()
                .appName("Spark SQL")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> vols_df = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/db_aeroport")
                .option("dbtable", "VOLS")
                .option("user", "root")
                .option("password", "")
                .load();

        Dataset<Row> passagers_df = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/db_aeroport")
                .option("dbtable", "PASSAGERS")
                .option("user", "root")
                .option("password", "")
                .load();

        Dataset<Row> reservations_df = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/db_aeroport")
                .option("dbtable", "RESERVATIONS")
                .option("user", "root")
                .option("password", "")
                .load();

        // Question 1: Afficher le nombre de passagers par vol
        Dataset<Row> resultat_question1 = vols_df.join(reservations_df, vols_df.col("ID_VOL").equalTo(reservations_df.col("ID_VOL")), "left")
                .groupBy(vols_df.col("ID_VOL"), vols_df.col("DATE_DEPART"))
                .agg(functions.count(reservations_df.col("ID_PASSAGER")).alias("NOMBRE"));

        resultat_question1.show();

        // Question 2: Afficher la liste des vols en cours
        Dataset<Row> resultat_question2 = vols_df.select(vols_df.col("ID_VOL"), vols_df.col("DATE_DEPART"), vols_df.col("DATE_ARRIVE"));
        resultat_question2.show();

        // Fermeture de la session Spark
        spark.stop();
    }
}
