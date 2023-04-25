package com.gpcoder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Ex8FillNa {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("demoTemporaryViewSql")
                .master("local[*]")
                .getOrCreate();

        List<Row> inMemory = new ArrayList<>();
        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

        StructField[] fields = new StructField[] {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };

        StructType schema = new StructType(fields);
        Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);

        dataset.createOrReplaceTempView("logging_table");

        dataset.select(date_format(col("datetime"), "y").alias("year"), col("level"))
                .groupBy("level")
                .pivot("year")
                .agg(collect_list("year"))
                .na()
                .fill(0)
                .show();

        spark.close();

        /**
         * Output:
         *
         * +-----+------+------------+
         * |level|  2015|        2016|
         * +-----+------+------------+
         * | WARN|    []|[2016, 2016]|
         * |FATAL|[2015]|      [2016]|
         * | INFO|[2015]|          []|
         * +-----+------+------------+
         */
    }
}
