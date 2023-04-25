package com.gpcoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Example of Resilient Distributed Dataset (RDD)
 *
 * Reference: https://spark.apache.org/docs/latest/rdd-programming-guide.html
 */
public class Ex1MapReduce {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> inputData = Arrays.asList(36, 9, 121, 18);
        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        Integer result = myRdd.reduce(Integer::sum);
        System.out.println("Sum=" + result); // Sum=184

        JavaRDD<Double> sqrtRdd = myRdd.map(Math::sqrt);
        sqrtRdd.foreach(item -> System.out.println("Sqrt=" + item)); // Replace method reference by lambda expression got an error java.io.NotSerializableException: java.io.PrintStream

        /**
         * Output:
         *
         * Sqrt=3.0
         * Sqrt=4.242640687119285
         * Sqrt=11.0
         * Sqrt=6.0
         */

        JavaRDD<Long> singleIntegerRdd = sqrtRdd.map(value -> 1L);
        Long count = singleIntegerRdd.reduce(Long::sum);
        System.out.println("Count=" + count); // Count=4

        sc.close();
    }
}
