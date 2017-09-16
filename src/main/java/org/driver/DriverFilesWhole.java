package org.driver;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.driver.function.WordCounter;
import scala.Tuple2;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * Created by pedromorfeu on 18/06/2017.
 */
public class DriverFilesWhole {

    public static void main(String[] args) {
        String master = "local[*]";
//        String master = "spark://10.211.55.101:7077";
        SparkConf conf = new SparkConf()
                .setMaster(master)
                .setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("target/SparkTraining-1.0-SNAPSHOT.jar");

        long start = System.currentTimeMillis();

        // I think there won't be copying the file through the network
        // If that was true, then using an unexisting node file would work, because it would be copied from the driver
        JavaRDD<String> stringJavaRDD = sc.textFile("/spark_input_files/books/pg26404_new.txt");
        System.out.println(stringJavaRDD.count());
        JavaRDD<String> mappedRDD = stringJavaRDD
                .map(v1 -> {
                    return v1 + " " + InetAddress.getLocalHost();
                });
//        List<String> collect = mappedRDD.collect().subList(0, 100);
//        for (String s : collect){
//            System.out.println(s);
//        }

        JavaPairRDD<String, String> rdd = sc.wholeTextFiles("/spark_input_files/books/50430-0.txt");
        System.out.println(rdd.count());

        JavaPairRDD<String, Integer> listRDD = rdd.mapValues(s -> s.split(" ").length).cache();
        System.out.println(listRDD.count());
        System.out.println(listRDD.take(5));

        System.out.println("Took " + (System.currentTimeMillis() - start));

    }
}
