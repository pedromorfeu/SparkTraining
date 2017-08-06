package org.driver;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Collectors;

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
//                .set("spark.task.cpus", "2")
                .setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("target/SparkTraining-1.0-SNAPSHOT.jar");

        JavaRDD<Integer> list = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        System.out.println(list.count());

        new Thread(() -> {
            try {
                Thread.sleep(1000);
                JavaRDD<String> list1 = sc.parallelize(Arrays.asList(1, 2, 3)).map(integer -> 1 + ": " + InetAddress.getLocalHost().getHostAddress());
                System.out.println(list1.collect());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        new Thread(() -> {
            try {
                Thread.sleep(1000);
                JavaRDD<String> list1 = sc.parallelize(Arrays.asList(1, 2, 3, 4)).map(integer -> 2 + ": " + InetAddress.getLocalHost().getHostAddress());
                System.out.println(list1.collect());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        new Thread(() -> {
            try {
                Thread.sleep(1000);
                JavaRDD<String> list1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5)).map(integer -> 3 + ": " + InetAddress.getLocalHost().getHostAddress());
                System.out.println(list1.collect());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        new Thread(() -> {
            try {
                Thread.sleep(1000);
                JavaRDD<String> list1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6)).map(integer -> 4 + ": " + InetAddress.getLocalHost().getHostAddress());
                System.out.println(list1.collect());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        new Thread(() -> {
            try {
                Thread.sleep(1000);
                JavaRDD<String> list1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7)).map(integer -> 5 + ": " + InetAddress.getLocalHost().getHostAddress());
                System.out.println(list1.collect());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void printResult(JavaRDD<Integer> list1, int i) throws IOException {
        System.out.println(i + ": " + list1.count() + Files.list(Paths.get("/")).map(path -> path.toFile().getName()).collect(Collectors.toList()));
    }

}
