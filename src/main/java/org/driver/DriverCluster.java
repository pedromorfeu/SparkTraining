package org.driver;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by pedromorfeu on 18/06/2017.
 */
public class DriverCluster {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("spark://192.168.0.171:7077")
                .set("spark.driver.memory", "512m")
                .set("spark.worker.memory", "512m")
                .set("spark.executor.memory", "512m")
                .setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("target/SparkTraining-1.0-SNAPSHOT.jar");

        JavaRDD<Integer> list = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        System.out.println(list.count());
    }
}
