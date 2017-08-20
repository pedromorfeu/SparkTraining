package org.driver;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.driver.function.WordCounter;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by pedromorfeu on 18/06/2017.
 */
public class DriverAccumulator {

    public static void main(String[] args) {
//        String master = "local[8]";
        String master = "spark://10.211.55.101:7077";
        SparkConf conf = new SparkConf()
                .setMaster(master)
                .setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("target/SparkTraining-1.0-SNAPSHOT.jar");

        final Accumulator<Integer> accumulator = sc.accumulator(0);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
        rdd = rdd.filter(i -> {
            if (i < 5) {
                accumulator.add(10);
                return true;
            }
            return false;
        });

        System.out.println(rdd.count());
        System.out.println(accumulator.value());
    }
}
