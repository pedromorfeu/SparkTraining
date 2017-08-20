package org.driver;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.driver.function.WordCounter;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by pedromorfeu on 18/06/2017.
 */
public class DriverFilesAggregator {

    public static void main(String[] args) {
//        String master = "local[8]";
        String master = "spark://10.211.55.101:7077";
        SparkConf conf = new SparkConf()
                .setMaster(master)
//                .set("spark.driver.memory", "512m")
//                .set("spark.worker.memory", "512m")
//                .set("spark.executor.memory", "1512m")
//                .set("spark.task.cpus", "2")
                .setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("target/SparkTraining-1.0-SNAPSHOT.jar");

        JavaRDD<String> rdd = sc.parallelize(
                Arrays.asList(
                        "/home/vagrant/books/pg15532.txt",
                        "/home/vagrant/books/pg26404.txt",
                        "/home/vagrant/books/pg29640.txt"));
        JavaRDD<Tuple2<Integer, String>> map = rdd.map(new WordCounter()).cache();
        List<Tuple2<Integer, String>> collect = map.collect();
        for (Tuple2<Integer, String> t : collect) {
            System.out.println(t._1 + " " + t._2);
        }

        Integer reduce = map.map(t -> t._1).reduce((a, b) -> a + b);
        System.out.println(reduce);
    }
}
