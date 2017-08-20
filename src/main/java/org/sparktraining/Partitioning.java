package org.sparktraining;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by pedromorfeu on 08/05/2017.
 */
public class Partitioning {

    public static void main(String... args) {

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);

        long start = System.currentTimeMillis();

        ArrayList<Tuple2<Integer, String>> list1 = new ArrayList<>();
        for (int i = 0; i < 1_000_000; i++) {
            list1.add(new Tuple2<>(i, "hi1 " + i));
        }
        ArrayList<Tuple2<Integer, String>> list2 = new ArrayList<>();
        for (int i = 100; i < 500; i++) {
            list2.add(new Tuple2<>(i, "hi2 " + i));
        }

        JavaPairRDD<Integer, String> rdd1 = sc.parallelizePairs(list1)
                .partitionBy(new HashPartitioner(8))
                .cache();

        JavaPairRDD<Integer, String> rdd2 = sc.parallelizePairs(list2);
        JavaPairRDD<Integer, Tuple2<String, String>> join = rdd1.join(rdd2);
        join.saveAsTextFile("target/out" + System.currentTimeMillis());

        System.out.println("took " + (System.currentTimeMillis() - start));
    }

}
