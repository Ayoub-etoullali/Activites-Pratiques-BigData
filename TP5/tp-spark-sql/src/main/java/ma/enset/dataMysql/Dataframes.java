package ma.enset.dataMysql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.countDistinct;

public class Dataframes {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss=SparkSession.builder().master("local[*]").appName("Traitement de données stockées dans Mysql [Dataset,Dataframe]").getOrCreate();

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

        System.out.println("\n\n----------------Afficher le nombre de consultation par médecin----------------\n");
        Dataset<Row> df2_1 = ss.read().format("jdbc").options(options).option("dbtable","CONSULTATIONS").load();
        Dataset<Row> df2_2 = ss.read().format("jdbc").options(options).option("dbtable","MEDECINS").load();
        Dataset<Row> f2_merge=df2_1.join(df2_2);
        //f2_merge.show();
        f2_merge.select("NOM","PRENOM").groupBy("NOM","PRENOM").count().as("NOMBRE DE CONSULTATION").show();

        System.out.println("\n\n----------------Afficher pour chaque médecin, le nombre de patients qu’il a assisté----------------\n");
        Dataset<Row> df3_1 = ss.read().format("jdbc").options(options).option("dbtable","MEDECINS").load();
        Dataset<Row> df3_2 = ss.read().format("jdbc").options(options).option("dbtable","CONSULTATIONS").load();
        Dataset<Row> f3_merge=df3_1.join(df3_2);
        f2_merge.select("NOM","PRENOM","ID_PATIENT").groupBy("NOM","PRENOM","ID_PATIENT").agg(countDistinct("ID_PATIENT").as("NOMBRE DE PATIENT")).show();
    }
}
