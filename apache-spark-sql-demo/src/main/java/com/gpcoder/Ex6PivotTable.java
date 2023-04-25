package com.gpcoder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class Ex6PivotTable {

    private static final String DATA_FOLDER = "apache-spark-sql-demo/src/main/resources/";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("demoTemporaryViewSql")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv(DATA_FOLDER + "biglog.txt");

        dataset = dataset.select(
                col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType)
        );

        Object[] months = new Object[] { "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
        List<Object> columns = Arrays.asList(months);

        dataset = dataset.groupBy("level")
                .pivot("month", columns)
                .count();

        dataset.show(100);

        spark.close();

        /**
         * Output:
         *
         * +-----+-------+--------+-----+-----+----+----+----+------+---------+-------+--------+--------+
         * |level|January|February|March|April| May|June|July|August|September|October|November|December|
         * +-----+-------+--------+-----+-----+----+----+----+------+---------+-------+--------+--------+
         * | INFO|   2912|    2945| 2928| 2909|2871|2966|2920|  2977|     2826|   2929|    2320|    2901|
         * |ERROR|    439|     433|  392|  428| 425| 369| 408|   413|      399|    365|     334|     436|
         * | WARN|    763|     846|  821|  848| 802| 844| 849|   850|      841|    878|     637|     800|
         * |FATAL|     11|       6|    4|    8|   8|null|  10|    11|        9|      9|    1649|      11|
         * |DEBUG|   4191|    4260| 4080| 4266|4152|4165|4129|  4239|     4216|   4163|    3324|    4054|
         * +-----+-------+--------+-----+-----+----+----+----+------+---------+-------+--------+--------+
         */
    }
}
