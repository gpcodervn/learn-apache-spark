package com.gpcoder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class Ex7Aggregation {

    private static final String DATA_FOLDER = "apache-spark-sql-demo/src/main/resources/exams/";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("demoDataset")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv(DATA_FOLDER + "students.csv");

        dataset = dataset.groupBy("subject")
                .agg(
                        max(col("score").cast(DataTypes.IntegerType)).alias("max score"),
                        min(col("score").cast(DataTypes.IntegerType)).alias("min score"),
                        round(avg(col("score").cast(DataTypes.IntegerType)), 2).alias("avg score"),
                        round(stddev(col("score")), 2).alias("stddev")
                );

        dataset.show();

        /**
         * Output:
         *
         * +----------+---------+---------+---------+------+
         * |   subject|max score|min score|avg score|stddev|
         * +----------+---------+---------+---------+------+
         * |Philosophy|       98|        1|    55.77|  19.8|
         * |      Math|      100|        1|    55.41| 19.37|
         * | Chemistry|       99|        0|    55.65| 19.61|
         * |   English|       98|        1|     55.5| 19.63|
         * |   Spanish|       98|        1|    55.63| 19.73|
         * |   Italian|       98|        1|    56.11| 19.66|
         * |   History|       98|        1|    55.51| 19.53|
         * |  Classics|       98|        0|    55.69| 19.63|
         * |Modern Art|      100|        1|    55.94| 19.47|
         * |    French|       98|        1|    55.29| 19.27|
         * | Geography|       98|        1|    55.72|  19.5|
         * |   Physics|       98|        1|    55.45| 19.58|
         * |    German|       99|        1|    56.06|  19.6|
         * |   Biology|       99|        1|    55.54|  19.8|
         * +----------+---------+---------+---------+------+
         */
    }
}
