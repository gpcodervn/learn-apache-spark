package com.gpcoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Ex3Pair {

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
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0] , 1L  ))
                .reduceByKey((value1, value2) -> value1 + value2)
                .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        /**
         * Output:
         *
         * ERROR has 1 instances
         * INFO has 3 instances
         * WARN has 1 instances
         */

        sc.close();
    }
}
