package com.gpcoder.stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.gpcoder.kafkaproducer.ViewReportsSimulator.VIEW_RECORDS_TOPIC;

/**
 * Steps:
 * - Start docker: At folder apache-spark-streaming-demo/src/main/docker, run command `docker-compose up`
 * - Run this application
 * - Run Simulator to produce message to Kafka: {@link com.gpcoder.kafkaproducer.ViewReportsSimulator}
 *
 * Reference: https://spark.apache.org/docs/latest/streaming-programming-guide.html
 */
public class Ex2KafkaStream {

    private static final Duration WINDOW_DURATION = Durations.minutes(2);
    private static final Duration SLIDE_DURATION = Durations.seconds(30);
    private static final Duration BATCH_DURATION = Durations.milliseconds(300);

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("demoApacheSparkStreaming");

        try (JavaStreamingContext sc = new JavaStreamingContext(conf, BATCH_DURATION)) {

            Map<String, Object> kafkaConfig = new HashMap<>();
            kafkaConfig.put("bootstrap.servers", "localhost:29092");
            kafkaConfig.put("key.deserializer", StringDeserializer.class);
            kafkaConfig.put("value.deserializer", StringDeserializer.class);
            kafkaConfig.put("group.id", "spark-consumer-group-demo");
            kafkaConfig.put("auto.offset.reset", "earliest");
            kafkaConfig.put("enable.auto.commit", true);

            Collection<String> kafkaTopics = Collections.singletonList(VIEW_RECORDS_TOPIC);

            JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(sc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(kafkaTopics, kafkaConfig));

            JavaPairDStream<Long, String> results = stream.mapToPair(item -> new Tuple2<>(item.value(), 5L))
                    .reduceByKeyAndWindow(Long::sum, WINDOW_DURATION, SLIDE_DURATION)
                    .mapToPair(Tuple2::swap)
                    .transformToPair(rdd -> rdd.sortByKey(false));

            System.out.println("Waiting for results ...");
            results.print(50);

            sc.start();
            sc.awaitTermination();
            // Check Spark Visualization: http://localhost:4040/
        }
    }
}
