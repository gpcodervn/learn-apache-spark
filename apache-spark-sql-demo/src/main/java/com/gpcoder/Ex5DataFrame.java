package com.gpcoder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class Ex5DataFrame {

    private static final String DATA_FOLDER = "apache-spark-sql-demo/src/main/resources/";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("demoTemporaryViewSql")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv(DATA_FOLDER + "biglog.txt");

        dataset = dataset.select(col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) );

        dataset = dataset.groupBy(col("level"),col("month"),col("monthnum")).count();
        dataset = dataset.orderBy(col("monthnum"), col("level"));
        dataset = dataset.drop(col("monthnum"));

        dataset.show(100);

        spark.close();
    }
}
