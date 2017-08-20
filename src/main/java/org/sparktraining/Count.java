package org.sparktraining;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by pedromorfeu on 08/05/2017.
 */
public class Count {

    public static void main(String... args) {

        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "a", "a", "c", "d", "b", "c"));
        Map<String, Long> map = rdd.countByValue();
        System.out.println(map);

    }

}
