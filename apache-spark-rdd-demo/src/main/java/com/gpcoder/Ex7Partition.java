package com.gpcoder;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Scanner;

public class Ex7Partition {

    private static final String DATA_FOLDER = "apache-spark-rdd-demo/src/main/resources/big-logs/";
    private static final String BIG_LOG_FILE_NAME = DATA_FOLDER + "bigLog.txt";
    private static final int NUMBER_OF_ROWS = 10000;
    private static final boolean DELETE_FILE_AFTER_RUN = true;

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        initBigData(NUMBER_OF_ROWS);

        JavaRDD<String> initialRdd = sc.textFile(BIG_LOG_FILE_NAME);

        System.out.println("Initial RDD Partition Size: " + initialRdd.getNumPartitions());

        JavaPairRDD<String, String> warningsAgainstDate = initialRdd.mapToPair(inputLine -> {
            String[] cols = inputLine.split(":");
            String level = cols[0];
            String date = cols[1];
            return new Tuple2<>(level, date);
        });

        System.out.println("After a narrow transformation we have " + warningsAgainstDate.getNumPartitions() + " parts");

        // Now we're going to do a "wide" transformation
        JavaPairRDD<String, Iterable<String>> results = warningsAgainstDate.groupByKey();

        results = results.persist(StorageLevel.MEMORY_AND_DISK());

        System.out.println(results.getNumPartitions() + " partitions after the wide transformation");

        results.foreach(it -> System.out.println("key " + it._1 + " has " + CollectionUtils.size(it._2) + " elements"));

        System.out.println("Completed Count=" + results.count());

        deleteFileIfRequired();

        // Access UI with url: http://localhost:4040/
        waitUntilStopApp();
        sc.close();

    }

    private static void waitUntilStopApp() {
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
    }

    private static void initBigData(long numberOfRows) throws IOException {
        System.out.println("Stat init big log data with file name " + BIG_LOG_FILE_NAME);
        final String logPattern = "%s: %s message=log%s \n";
        final String[] severities = {"ERROR", "WARN", "INFO"};

        FileUtils.createParentDirectories(new File(BIG_LOG_FILE_NAME));

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(BIG_LOG_FILE_NAME))) {
            for (int i = 1; i <= numberOfRows; i++) {
                String severity = severities[RandomUtils.nextInt(0,3)];
                String date = LocalDate.now().minusDays(RandomUtils.nextInt(1, 365)).toString();
                String log = String.format(logPattern, severity, date, i);

                writer.write(log);
                if (i % 1000 == 0) {
                    System.out.println("Write " + i + " rows");
                    writer.flush();
                }
            }
        } catch (IOException e) {
            System.out.println("ERROR init big log data with file name " + BIG_LOG_FILE_NAME);
            e.printStackTrace();
        } finally {
            System.out.println("Finished init big log data with file name " + BIG_LOG_FILE_NAME);
        }

    }

    private static void deleteFileIfRequired() throws IOException {
        if (DELETE_FILE_AFTER_RUN) {
            FileUtils.delete(new File(BIG_LOG_FILE_NAME));
            System.out.println("Deleted file " + BIG_LOG_FILE_NAME);
        }
    }
}
