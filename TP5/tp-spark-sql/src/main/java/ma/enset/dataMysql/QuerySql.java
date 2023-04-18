package ma.enset.dataMysql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

import java.util.HashMap;
import java.util.Map;

public class QuerySql {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss=SparkSession
                .builder()
                .master("local[*]")
                .appName("Traitement de données stockées dans Mysql [Query SQL]")
                .getOrCreate();

        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/DB_HOPITAL");
        options.put("user","root");
        options.put("password","");

        System.out.println("\n\n----------------Afficher le nombre de consultations par jour----------------\n");
        Dataset<Row> df1 = ss.read().format("jdbc")
                .options(options)
                //.option("dbtable","CONSULTATIONS")
//                .option("query","select DATE_CONSULTATION , count( from CONSULTATIONS group by DATE_CONSULTATION)")
                .option("query","SELECT DATE_CONSULTATION, COUNT(*) AS NB_CONSULTATIONS\n" +
                        "FROM CONSULTATIONS\n" +
                        "GROUP BY DATE_CONSULTATION")
                .load();
        df1.show();

        System.out.println("\n\n----------------Afficher le nombre de consultation par médecin----------------\n");
        Dataset<Row> df2 = ss.read().format("jdbc")
                .options(options)
                /*
                .option("query", "select medecins.NOM, medecins.PRENOM, COUNT(medecins.ID) AS nombre_de_consultations " +
                        "from consultations " +
                        "join medecins on medecins.ID=consultations.ID_MEDECIN " +
                        "group by consultations.ID_MEDECIN")
                 */
                .option("query","SELECT M.NOM, M.PRENOM, COUNT(*) AS NB_CONSULTATIONS\n" +
                        "FROM CONSULTATIONS C\n" +
                        "JOIN MEDECINS M ON C.ID_MEDECIN = M.ID\n" +
                        "GROUP BY M.NOM, M.PRENOM")
                .load();
        df2.show();


        System.out.println("\n\n----------------Afficher pour chaque médecin, le nombre de patients qu’il a assisté----------------\n");
        Dataset<Row> df3 = ss.read().format("jdbc")
                .options(options)
                /* [N.B] pas complet
                .option("query", "SELECT medecins.NOM, medecins.PRENOM, COUNT(patients.ID) AS nombre_de_patients\n" +
                        "FROM consultations\n" +
                        "JOIN medecins ON medecins.ID=consultations.ID_MEDECIN\n" +
                        "JOIN patients ON patients.ID=consultations.ID_PATIENT\n" +
                        "GROUP BY consultations.ID_MEDECIN, consultations.ID_PATIENT")
                 */
                .option("query", "SELECT M.NOM, M.PRENOM, COUNT(DISTINCT C.ID_PATIENT) AS NB_PATIENTS\n" +
                        "FROM CONSULTATIONS C\n" +
                        "JOIN MEDECINS M ON C.ID_MEDECIN = M.ID\n" +
                        "GROUP BY M.NOM, M.PRENOM")
                .load();
        df3.show();

    }
}
