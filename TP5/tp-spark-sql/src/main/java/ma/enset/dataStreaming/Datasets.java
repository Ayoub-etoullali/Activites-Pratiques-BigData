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

        Encoder<Hopital> hopitalEncoder = Encoders.bean(Hopital.class);
        Dataset<Hopital> dsHopital = dfLines.as(Encoders.STRING())
                .map((MapFunction<String, Hopital>) ligne -> {
                    String hopitalTab[] = ligne.split(" ");
                    Hopital hopital = new Hopital();
                    hopital.setId(Integer.parseInt(hopitalTab[0]));
                    hopital.setTitre(hopitalTab[1]);
                    hopital.setDescription(hopitalTab[2]);
                    hopital.setService(hopitalTab[3]);
                    hopital.setDate(new Date(hopitalTab[4]));
                    return hopital;
                }, hopitalEncoder);
        Dataset<Row> dfSum = dsHopital.groupBy("service").sum("Id");

        StreamingQuery streamingQuery = dfLines
                .writeStream()
                .format("console")
                .trigger(Trigger.ProcessingTime(5000))//Microbatch every 5 seconds
                .outputMode("append")//cas possible : append,complete or update
                .start();
        streamingQuery.awaitTermination();
    }
}
