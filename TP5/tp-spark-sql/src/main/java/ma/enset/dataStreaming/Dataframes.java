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

public class Dataframes {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("TP SPARK Structured Streaming [csv in HDFS]")
                .getOrCreate();

        StructType venteSchema = new StructType(new StructField[]{
                new StructField("Id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("titre", DataTypes.StringType, true, Metadata.empty()),
                new StructField("description", DataTypes.StringType, true, Metadata.empty()),
                new StructField("service", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Date", DataTypes.StringType, true, Metadata.empty()),
        });

        Dataset<Row> dfHopital = spark.readStream().format("csv")
                .option("header", true)
                .schema(venteSchema)
                .load("hdfs://localhost:19000/hopital");

        StreamingQuery streamingQuery = dfHopital
                .writeStream()
                .format("console")
                .trigger(Trigger.ProcessingTime(5000))//Microbatch every 5 seconds
                .outputMode("append")//append,complete or update
                .start();
        streamingQuery.awaitTermination();

        System.out.println("\n----------------Afficher d’une manière continue le nombre d’incidents par service----------------\n");
        dfHopital
                .groupBy("service")
                .count().as("Nombre d’incidents")
                .show();

        System.out.println("\n----------------Afficher d’une manière continue les deux année ou il a y avait plus d’incidents.----------------\n");

    }
}