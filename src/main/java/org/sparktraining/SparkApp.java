package org.sparktraining;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created by pedromorfeu on 08/05/2017.
 */
public class SparkApp {

    public static void main(String... args) {

        String logFile = "/Users/pedromorfeu/BigData/spark-1.6.2-bin-hadoop2.6/README.md"; // Should be some file on your system
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(logFile);

        System.out.println(lines.count());
        System.out.println(lines.first());

        JavaRDD<String> linespython = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("Python");
            }
        });

        System.out.println(linespython.count());
        System.out.println(linespython.first());

        JavaRDD<String> linespython1 = lines.filter(line -> line.contains("Python"));

        System.out.println(linespython1.count());
        System.out.println(linespython1.first());

    }

}
