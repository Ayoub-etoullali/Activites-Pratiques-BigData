package ma.enset.dataStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.Date;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.desc;

public class Datasets {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("TP SPARK Structured Streaming [console]")
                .getOrCreate();
        Dataset<Row> dfLines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9090)
                .load();

        Encoder<Incident> volEncoder = Encoders.bean(Incident.class);
        Dataset<Incident> dsvol = dfLines.as(Encoders.STRING())
                .map((MapFunction<String, Incident>) ligne -> {
                    String volTab[] = ligne.split(",");
                    Incident incident = new Incident();
                    incident.setId(Integer.parseInt(volTab[0]));
                    incident.setDescription(volTab[1]);
                    incident.setNom_avion(volTab[2]);
                    incident.setDate(new Date(volTab[3]));
                    return incident;
                }, volEncoder);

        // Calcul du nombre d'incidents par avion
        Dataset<Row> incidentsCount = dsvol.groupBy("nom_avion").count();

        // Sélection de l'avion ayant le plus d'incidents
        Dataset<Row> avionPlusIncidents = incidentsCount.orderBy(desc("count")).limit(1);

        // Affichage continu
        StreamingQuery query = avionPlusIncidents.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        // Attente de la fin du streaming
        query.awaitTermination();

        StreamingQuery streamingQuery = dfLines
                .writeStream()
                .format("console")
                .trigger(Trigger.ProcessingTime(5000))//Microbatch every 5 seconds
                .outputMode("append")//cas possible : append,complete or update
                .start();
        streamingQuery.awaitTermination();
    }
}
