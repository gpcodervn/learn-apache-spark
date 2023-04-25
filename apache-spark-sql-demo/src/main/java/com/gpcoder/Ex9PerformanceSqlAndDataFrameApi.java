package com.gpcoder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class Ex9PerformanceSqlAndDataFrameApi {

    private static final String DATA_FOLDER = "apache-spark-sql-demo/src/main/resources/";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("demoDataset")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv(DATA_FOLDER + "biglog.txt");

        dataset.createOrReplaceTempView("logging_table");

        /**
         * Using Apache Spark SQL
         */
//        Dataset<Row> results = spark.sql
//                ("select level, date_format(datetime,'MMMM') as month, count(1) as total " +
//                        "from logging_table " +
//                        "group by level, month " +
//                        "order by monthNumFunc(month), level"); // Bad performance
        // Use temporary column for sorting, then drop it. to improve performance.
        Dataset<Row> results = spark.sql
                ("select level, date_format(datetime,'MMMM') as month, count(1) as total, " +
                        "   date_format(datetime,'M') as monthnum " + // 1. Create a temporary column
                        "from logging_table " +
                        "group by level, month, monthnum " +
                        "order by monthnum, level");
        results = results.drop("monthnum"); // 2. Drop a temporary column


        /**
         * Using Data Frame API (Dataset)
         */
//		dataset = dataset.select(col("level"),
//				                 date_format(col("datetime"), "MMMM").alias("month"),
//				                 date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) );
//
//		dataset = dataset.groupBy("level","month","monthnum").count().as("total").orderBy("monthnum");
//        results = dataset.drop("monthnum");

        results.show(100);

        results.explain();

//		Scanner scanner = new Scanner(System.in);
//		scanner.nextLine();

        spark.close();
    }
}
