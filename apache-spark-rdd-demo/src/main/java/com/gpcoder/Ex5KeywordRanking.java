package com.gpcoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Ex5KeywordRanking {

    private static final String DATA_FOLDER = "apache-spark-rdd-demo/src/main/resources/count-words/";
    private static final List<String> IGNORED_WORDS = Arrays.asList("Lorem", "Ipsum");

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile(DATA_FOLDER + "input-words.txt");

        JavaRDD<String> lettersOnlyRdd = initialRdd.map( sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase() );

        JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter(sentence -> sentence.trim().length() > 0);

        JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());

        JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);

        JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(word -> !IGNORED_WORDS.contains(word));

        JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair(word -> new Tuple2<>(word, 1L));

        JavaPairRDD<String, Long> totals = pairRdd.reduceByKey(Long::sum);

        JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));

        JavaPairRDD<Long, String> sorted = switched.sortByKey(false);

        List<Tuple2<Long, String>> results = sorted.take(10);
        results.forEach(word -> System.out.println(word));

        sc.close();
    }
}
