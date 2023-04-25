package com.gpcoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Ex8Join {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> visitsRaw = Arrays.asList(
                new Tuple2<>(4, 20),
                new Tuple2<>(1, 8),
                new Tuple2<>(10, 9)
        );

        List<Tuple2<Integer, String>> usersRaw = Arrays.asList(
                new Tuple2<>(1, "GP 1"),
                new Tuple2<>(2, "GP 2"),
                new Tuple2<>(3, "GP 3"),
                new Tuple2<>(4, "GP 4"),
                new Tuple2<>(5, "GP 5")
        );

        JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

        System.out.println("Inner join");
        JavaPairRDD<Integer, Tuple2<Integer, String>> joined = visits.join(users);
        joined.foreach(tuple -> System.out.println(tuple));

        System.out.println("Left join");
        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftJoined = visits.leftOuterJoin(users);
        leftJoined.foreach(tuple -> System.out.println(tuple));

        System.out.println("Right join");
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightedJoined = visits.rightOuterJoin(users);
        rightedJoined.foreach(tuple -> System.out.println(tuple));

        System.out.println("Full join");
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> fullJoined = visits.cartesian(users);
        fullJoined.foreach(tuple -> System.out.println(tuple));
    }
}
