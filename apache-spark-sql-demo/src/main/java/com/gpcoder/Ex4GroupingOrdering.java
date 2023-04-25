package com.gpcoder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Ex4GroupingOrdering {

    private static final String DATA_FOLDER = "apache-spark-sql-demo/src/main/resources/";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("demoTemporaryViewSql")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv(DATA_FOLDER + "biglog.txt");

        dataset.createOrReplaceTempView("logging_table");

        Dataset<Row> results = spark.sql
                ("select level, date_format(datetime,'MMMM') as month, count(1) as total " +
                        "from logging_table " +
                        "group by level, month " +
                        "order by cast(first(date_format(datetime,'M')) as int), level");

        results.show(100);

        spark.close();

        /**
         * Output:
         *
         * +-----+---------+-----+
         * |level|    month|total|
         * +-----+---------+-----+
         * |DEBUG|  January| 4191|
         * |ERROR|  January|  439|
         * |FATAL|  January|   11|
         * | INFO|  January| 2912|
         * | WARN|  January|  763|
         * |DEBUG| February| 4260|
         * |ERROR| February|  433|
         * |FATAL| February|    6|
         * | INFO| February| 2945|
         * | WARN| February|  846|
         * |DEBUG|    March| 4080|
         * |ERROR|    March|  392|
         * |FATAL|    March|    4|
         * | INFO|    March| 2928|
         * | WARN|    March|  821|
         * |DEBUG|    April| 4266|
         * |ERROR|    April|  428|
         */
    }
}
