package com.gpcoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Ex4FlatMap {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> inputData = Arrays.asList(
                "INFO: 23/04/23 10:58:19",
                "WARN: 23/04/23 10:58:21",
                "INFO: 23/04/23 11:00:55",
                "ERROR: 23/04/23 10:11:33",
                "INFO: 24/04/23 03:58:00"
        );
        sc.parallelize(inputData)
                .flatMap(value -> Arrays.asList(value.split(":")[0]).iterator())
                .filter(word -> word.length() > 1)
                .distinct()
                .foreach(word -> System.out.println(word));

        /**
         * Output:
         *
         * ERROR
         * INFO
         * WARN
         */

        sc.close();
    }
}
