package org.sparktraining;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by pedromorfeu on 08/05/2017.
 */
public class Types {

    public static void main(String... args) {

        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
        JavaDoubleRDD javaDoubleRDD = rdd.mapToDouble(x -> (double) x * x);
        System.out.println(javaDoubleRDD.collect());
        System.out.println(javaDoubleRDD.mean());

        JavaRDD<Double> map = rdd.map(x -> (double) x * x);
        System.out.println(map.collect());

    }

}
