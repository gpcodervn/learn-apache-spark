package com.gpcoder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Ex2TemporaryViewSql {

    private static final String DATA_FOLDER = "apache-spark-sql-demo/src/main/resources/exams/";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("demoTemporaryViewSql")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv(DATA_FOLDER + "students.csv");

        dataset.createOrReplaceTempView("my_students_table");

        Dataset<Row> results = spark.sql("select distinct(year) from my_students_table order by year desc");
        results.show();

        spark.close();
    }
}
