package org.sparktraining;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.driver.function.Executor;

import java.util.Arrays;
import java.util.List;

/**
 * Created by pedromorfeu on 18/06/2017.
 */
public class Coalescing {

    public static void main(String[] args) {
//        String master = "local[8]";
        String master = "spark://10.211.55.101:7077";
        SparkConf conf = new SparkConf()
                .setMaster(master)
                .setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("target/SparkTraining-1.0-SNAPSHOT.jar");

        JavaRDD<String> mainRDD = sc.textFile("/spark_input_files/books/pg26404_new.txt");
        System.out.println("num partitions: " + mainRDD.getNumPartitions());
        System.out.println("count: " + mainRDD.count());

        JavaRDD<String> filteredRDD = mainRDD.filter(v1 -> !v1.trim().isEmpty() && v1.contains("notario"));
        System.out.println("num partitions: " + filteredRDD.getNumPartitions());
        System.out.println("count: " + filteredRDD.count());

        JavaRDD<String> coalescedRDD = filteredRDD.coalesce(4);
        System.out.println("num partitions: " + coalescedRDD.getNumPartitions());
        System.out.println("count: " + coalescedRDD.count());

    }
}
