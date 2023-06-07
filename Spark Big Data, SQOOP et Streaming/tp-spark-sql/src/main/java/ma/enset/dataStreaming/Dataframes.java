package ma.enset.dataStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.desc;

public class Dataframes {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("TP SPARK Structured Streaming [csv in HDFS]")
                .getOrCreate();

        StructType venteSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("nom_avion", DataTypes.StringType, true, Metadata.empty()),
                new StructField("description", DataTypes.StringType, true, Metadata.empty()),
                new StructField("date", DataTypes.StringType, true, Metadata.empty()),
        });

        Dataset<Row> dfvol = spark.readStream().format("csv")
                .option("header", true)
                .schema(venteSchema)
                .load("hdfs://localhost:9000/incidents");

        StreamingQuery streamingQuery = dfvol
                .writeStream()
                .format("console")
                .trigger(Trigger.ProcessingTime(5000))//Microbatch every 5 seconds
                .outputMode("append")//append,complete or update
                .start();
        streamingQuery.awaitTermination();

        System.out.println("\n----------------Afficher d’une manière continue l’avion ayant plus d’incidents----------------\n");
        dfvol
                .groupBy("nom_avion")
                .count().as("Nombre d’incidents")
                .show();

        // Calcul du nombre d'incidents par avion
        Dataset<Row> incidentsCount = dfvol.groupBy("nom_avion").count();

        // Sélection de l'avion ayant le plus d'incidents
        Dataset<Row> avionPlusIncidents = incidentsCount.orderBy(desc("count")).limit(1);

        // Affichage continu
        StreamingQuery query = avionPlusIncidents.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        // Attente de la fin du streaming
        query.awaitTermination();

    }
}