package com.gpcoder.stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

import static com.gpcoder.kafkaproducer.ViewReportsSimulator.VIEW_RECORDS_TOPIC;

/**
 * Steps:
 * - Start docker: At folder apache-spark-streaming-demo/src/main/docker, run command `docker-compose up`
 * - Run this application
 * - Run Simulator to produce message to Kafka: {@link com.gpcoder.kafkaproducer.ViewReportsSimulator}
 *
 * Reference: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
 */
public class Ex3StructuredStream {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {

        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .appName("demoStructuredStreaming")
                .getOrCreate();

        session.conf().set("spark.sql.shuffle.partitions", "10");

        Dataset<Row> df = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:29092")
                .option("subscribe", VIEW_RECORDS_TOPIC)
                .load();

        // start some dataframe operations
        df.createOrReplaceTempView("viewing_figures");

        // key, value, timestamp
        Dataset<Row> results =
                session.sql("select window, cast (value as string) as course_name, sum(5) as seconds_watched " +
                        "from viewing_figures " +
                        "group by window(timestamp,'2 minutes'),course_name");

        System.out.println("Waiting for results ...");
        StreamingQuery query = results
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Update())
                .option("truncate", false)
                .option("numRows", 50)
                .start();

        query.awaitTermination();
        // Check Spark Visualization: http://localhost:4040/
    }
}
