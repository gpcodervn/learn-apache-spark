package com.gpcoder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;

public class Ex8Udf {

    private static final String DATA_FOLDER = "apache-spark-sql-demo/src/main/resources/";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("demoDataset")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv(DATA_FOLDER + "biglog.txt");

        SimpleDateFormat input = new SimpleDateFormat("MMMM");
        SimpleDateFormat output = new SimpleDateFormat("M");

        spark.udf().register("monthNumFunc", (String month) -> {
            java.util.Date inputDate = input.parse(month);
            return Integer.parseInt(output.format(inputDate));
        }, DataTypes.IntegerType);


        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> results = spark.sql
                ("select level, date_format(datetime,'MMMM') as month, count(1) as total " +
                        "from logging_table " +
                        "group by level, month " +
                        "order by monthNumFunc(month), level");

        results.show(100);

        spark.close();
    }
}
