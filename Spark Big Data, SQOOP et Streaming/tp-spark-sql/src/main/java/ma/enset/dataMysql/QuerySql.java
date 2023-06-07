package ma.enset.dataMysql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class QuerySql {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss = SparkSession
                .builder()
                .master("local[*]")
                .appName("Traitement de données stockées dans Mysql [Query SQL]")
                .getOrCreate();

        Map<String, String> options = new HashMap<>();
        options.put("driver", "com.mysql.cj.jdbc.Driver");
        options.put("url", "jdbc:mysql://localhost:3306/db_aeroport");
        options.put("user", "root");
        options.put("password", "");

        // Question 1: Afficher le nombre de passagers par vol
        Dataset<Row> df1 = ss.read().format("jdbc")
                .options(options)
                .option("query", "SELECT v.ID, v.DATE_DEPART, COUNT(r.ID_PASSAGER) AS NOMBRE\n" +
                        "FROM VOLS v\n" +
                        "LEFT JOIN RESERVATIONS r ON v.ID = r.ID_VOL\n" +
                        "GROUP BY v.ID, v.DATE_DEPART;\n")
                .load();
        df1.show();
        // Question 2: Afficher la liste des vols en cours
        Dataset<Row> df2 = ss.read().format("jdbc")
                .options(options)
                .option("query", "SELECT ID_VOL, DATE_DEPART, DATE_ARRIVE\n" +
                        "FROM VOLS;\n")
                .load();
        df2.show();
    }
}
