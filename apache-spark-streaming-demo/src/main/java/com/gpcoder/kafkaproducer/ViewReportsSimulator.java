package com.gpcoder.kafkaproducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import static com.gpcoder.kafkaproducer.DataUtils.initCourseKeys;

public class ViewReportsSimulator {

    public static final String VIEW_RECORDS_TOPIC = "viewrecords";
    private static final String DATA_FOLDER = "apache-spark-streaming-demo/src/main/resources/sampledata/";

    public static void main(String[] args) throws InterruptedException, FileNotFoundException {
        System.out.println("Started ViewReportsSimulator app");
        Properties kafkaConfig = new Properties();
        kafkaConfig.put("acks", "all"); // See https://kafka.apache.org/documentation/
        kafkaConfig.put("retries", 0);
        kafkaConfig.put("batch.size", 16384);
        kafkaConfig.put("linger.ms", 1);
        kafkaConfig.put("buffer.memory", 33554432);
        kafkaConfig.put("bootstrap.servers", "localhost:29092");
        kafkaConfig.put("key.serializer", StringSerializer.class);
        kafkaConfig.put("value.serializer", StringSerializer.class);

        Map<Integer, String> courseKeys = initCourseKeys();

        // The input file contains a full day (24 hours) of viewing figures. We'll speed this up
        // by 24x to give a one hour simulation....
        try (Producer<String, String> producer = new KafkaProducer<>(kafkaConfig);
                Scanner sc = new Scanner(new FileReader(DATA_FOLDER + "final_viewing_figures.txt"))) {
            System.out.println("Reading file and sending to Kafka ...");
            int milliseconds = 0;
            while (sc.hasNextLine()) {
                String[] input = sc.nextLine().split(",");
                Integer timestamp = Integer.parseInt(input[0]);
                Integer courseKey = Integer.parseInt(input[1]);

                // Wait until this event is due...
                while (milliseconds < timestamp) {
                    milliseconds++;
                    if (milliseconds % 24 == 0) {
                        TimeUnit.MILLISECONDS.sleep(1);
                    }
                }

                String courseName = courseKeys.get(courseKey);
                producer.send(
                        new ProducerRecord<>(VIEW_RECORDS_TOPIC, courseName),
                        (metadata, exception) -> System.out.println("Sent courseName=" + courseName)
                );
            }
            System.out.println("Sent all events to Kafka");
        }
    }
}
