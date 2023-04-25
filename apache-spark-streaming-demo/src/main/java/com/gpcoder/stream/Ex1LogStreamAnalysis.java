package com.gpcoder.stream;

import com.gpcoder.serverlog.LoggingServer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import static com.gpcoder.serverlog.LoggingServer.LOG_SEPARATOR;

/**
 * Before running this application, you must run {@link LoggingServer} first.
 *
 * After run this application, waiting in 1 minutes (WINDOW_DURATION), then we can see a result like this:
 *
 * (ERROR,11336)
 * (INFO,11616)
 * (WARN,11466)
 * (DEBUG,11517)
 */
public class Ex1LogStreamAnalysis {

    private static final Duration WINDOW_DURATION = Durations.minutes(1);
    private static final Duration BATCH_DURATION = Durations.seconds(2);

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("demoApacheSparkStreaming");

        try(JavaStreamingContext sc = new JavaStreamingContext(conf, BATCH_DURATION)) {

            JavaReceiverInputDStream<String> inputData = sc.socketTextStream("localhost", LoggingServer.PORT);

            JavaDStream<String> results = inputData.map(item -> item);
            JavaPairDStream<String, Long> pairDStream = results.mapToPair(rawLogMessage -> new Tuple2<>(rawLogMessage.split(LOG_SEPARATOR)[0], 1L));
            pairDStream = pairDStream.reduceByKeyAndWindow(Long::sum, WINDOW_DURATION);

            pairDStream.print();

            sc.start();
            sc.awaitTermination();
        }
    }
}
