package org.driver;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by pedromorfeu on 18/06/2017.
 */
public class DriverClusterConcurrent {
    public static void main(String[] args) {
//        String master = "spark://192.168.0.171:7077";
        String master = "spark://10.211.55.101:7077";
        SparkConf conf = new SparkConf()
                .setMaster(master)
//                .set("spark.driver.memory", "512m")
//                .set("spark.worker.memory", "512m")
//                .set("spark.executor.memory", "512m")
                .setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("target/SparkTraining-1.0-SNAPSHOT.jar");

        JavaRDD<Integer> list = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        System.out.println(list.count());

        new Thread(() -> {
            JavaRDD<Integer> list1 = sc.parallelize(Arrays.asList(1, 2, 3));
            System.out.println(list1.count());
        }).start();
        new Thread(() -> {
            JavaRDD<Integer> list1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
            System.out.println(list1.count());
        }).start();
        new Thread(() -> {
            JavaRDD<Integer> list1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
            System.out.println(list1.count());
        }).start();
        new Thread(() -> {
            JavaRDD<Integer> list1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));
            System.out.println(list1.count());
        }).start();
        new Thread(() -> {
            JavaRDD<Integer> list1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
            System.out.println(list1.count());
        }).start();
    }
}
