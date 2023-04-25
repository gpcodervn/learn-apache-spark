package com.gpcoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Ex2Tuple {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> inputData = Arrays.asList(36, 9, 121, 18);
        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        JavaRDD<Tuple2<Integer, Double>> sqrtRdd = myRdd.map(value -> new Tuple2<>(value, Math.sqrt(value)) );
        sqrtRdd.foreach(tuple -> System.out.println("sqrt(" + tuple._1 + ")=" + tuple._2));

        /**
         * Output:
         *
         * sqrt(9)=3.0
         * sqrt(121)=11.0
         * sqrt(36)=6.0
         * sqrt(18)=4.242640687119285
         */

        sc.close();
    }
}
