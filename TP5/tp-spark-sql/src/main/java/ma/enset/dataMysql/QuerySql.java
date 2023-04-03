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
        SparkSession ss=SparkSession.builder().master("local[*]").appName("Traitement de données stockées dans Mysql [Dataset,Dataframe]").getOrCreate();

        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/DB_HOPITAL");
        options.put("user","root");
        options.put("password","");

        System.out.println("\n\n----------------Afficher le nombre de consultations par jour----------------\n");
        Dataset<Row> df1 = ss.read().format("jdbc")
                .options(options)
                //.option("dbtable","CONSULTATIONS")
                .option("query","select DATE_CONSULTATION , count( from CONSULTATIONS group by DATE_CONSULTATION)")
                .load();
        df1.show();

        System.out.println("\n\n----------------Afficher le nombre de consultation par médecin----------------\n");

        System.out.println("\n\n----------------Afficher pour chaque médecin, le nombre de patients qu’il a assisté----------------\n");


        /*Dataset<Row> dfEmp = ss.read().format("jdbc")
                .options(options)
                //.option("dbtable", "EMPLOYES")
                .option("query","select ID,NOM,PRENOM from PATIENTS")
                .load();
        dfEmp.printSchema();
        dfEmp.show();*/
    }
}
