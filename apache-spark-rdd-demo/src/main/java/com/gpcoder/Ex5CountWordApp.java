package com.gpcoder;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Ex5CountWordApp {

    private static final String INPUT = "Hello Spark.\n This is an example with Spark with Java.\n Hello, Hello Spark.";

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("JavaWordCount")
                .setMaster("local[*]");

        try (JavaSparkContext ctx = new JavaSparkContext(sparkConf);) {
            JavaRDD<String> lines = ctx.parallelize(Arrays.asList(
                    INPUT.replaceAll("[^a-zA-Z\\s]", "")
                            .toLowerCase()
                            .split("\n")
            ));

            JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

            JavaPairRDD<String, Integer> ones = words.filter(StringUtils::isNotBlank).
                    mapToPair(word -> new Tuple2<>(word.trim(), 1));

            JavaPairRDD<String, Integer> counts = ones.reduceByKey(Integer::sum);

            List<Tuple2<String, Integer>> output = counts.collect();
            for (Tuple2<?, ?> tuple : output) {
                System.out.println(tuple._1() + ": " + tuple._2());
            }

            ctx.stop();
        }
    }
}
