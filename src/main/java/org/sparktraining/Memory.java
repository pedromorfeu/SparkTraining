package org.sparktraining;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * Created by pedromorfeu on 18/06/2017.
 */
public class Memory {

    public static void main(String[] args) throws IOException {
//        String master = "local[8]";
        String master = "spark://10.211.55.101:7077";
        SparkConf conf = new SparkConf()
                .setMaster(master)
                .setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("target/SparkTraining-1.0-SNAPSHOT.jar");

        JavaRDD<String> mainRDD = sc
                .textFile("/spark_input_files/books/pg26404_new.txt")
                .cache();
        System.out.println("num partitions: " + mainRDD.getNumPartitions());
        System.out.println("count: " + mainRDD.count());

        System.in.read();

    }
}
