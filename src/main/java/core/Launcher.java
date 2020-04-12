
package core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Launcher {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL data sources example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        runBasicDataSourceExample(spark);

        spark.stop();
    }

    private static void runBasicDataSourceExample(SparkSession spark) {

        Dataset<Row> zipDF = spark.read().format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("/user/root/zipin/gaz2016zcta5distance500miles.csv");

        zipDF.printSchema();

        /*
        root
        |-- zip1: integer (nullable = true)
        |-- zip2: integer (nullable = true)
        |-- mi_to_zcta5: double (nullable = true)
        */

        Dataset<Row> sqlDF = spark.sql("SELECT DISTINCT COUNT(*) FROM root GROUP BY zip1");
        sqlDF.show();


    }

}