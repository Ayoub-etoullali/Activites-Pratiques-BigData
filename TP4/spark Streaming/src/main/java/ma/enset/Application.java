package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class Application {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTwitterHelloWorldExample");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
        System.setProperty("twitter4j.oauth.consumerKey", "cChZNFj6T5R0TigYB9yd1w");
        System.setProperty("twitter4j.oauth.consumerSecret", "53Qs77bvYdyRb9xUz67eW2ePmAd0z7muMNgFUQbuD8tRyece5L");
        System.setProperty("twitter4j.oauth.accessToken", "142020510-jNJqB3Ucdn57C2dD7EyDMxkNcOy8s8cmdRy6os9b");
        System.setProperty("twitter4j.oauth.accessTokenSecret", "zZIwVHGVHBvnrry2ttu2MvULJY86xZswsJlItPbTxvZQD");
//        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);
//        JavaDStream<String> statuses = twitterStream.map(status -> status.getText());
//        statuses.print();
        jssc.start();
        jssc.awaitTermination();
    }
}