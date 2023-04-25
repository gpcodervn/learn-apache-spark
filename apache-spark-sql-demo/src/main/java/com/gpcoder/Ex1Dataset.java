package com.gpcoder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

/**
 * Example of Spark SQL
 *
 * Reference: https://spark.apache.org/docs/latest/sql-programming-guide.html
 */
public class Ex1Dataset {

    private static final String DATA_FOLDER = "apache-spark-sql-demo/src/main/resources/exams/";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("demoDataset")
                .master("local[*]")
                .getOrCreate();


        StructField[] fields = new StructField[] {
                new StructField("student_id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("exam_center_id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("subject", DataTypes.StringType, false, Metadata.empty()),
                new StructField("year", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("quarter", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("score", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("grade", DataTypes.StringType, false, Metadata.empty()),
        };

        StructType schema = new StructType(fields);

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .schema(schema)
                .csv(DATA_FOLDER + "students.csv");

        System.out.println("Using Expression");
        Dataset<Row> result1 = dataset.filter("subject = 'Chemistry' AND year >= 2005 ");
        result1.show();


        System.out.println("Using Column");
        Dataset<Row> result2 = dataset.filter(
                col("subject").equalTo("Chemistry")
                        .and(col("year").geq(2005))
        );
        result2.show();

        spark.close();
    }
}
