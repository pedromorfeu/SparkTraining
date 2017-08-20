package org.sparktraining;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by pedromorfeu on 08/05/2017.
 */
public class Foreach {

    public static void main(String... args) {

        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
        rdd.foreach(integer -> System.out.println("hi " + integer));

    }

}
