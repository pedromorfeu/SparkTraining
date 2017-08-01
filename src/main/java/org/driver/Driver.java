package org.driver;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by pedromorfeu on 18/06/2017.
 */
public class Driver {
    public static void main(String[] args) {
//        SparkConf conf = new SparkConf()
//                .setMaster("local[4]")
//                .setAppName("Simple Application");
//        SparkConf conf = new SparkConf()
//                .setMaster("spark://10.211.55.101:7077")
//                .setAppName("Simple Application");
        SparkConf conf = new SparkConf()
                .setMaster("spark://192.168.0.171:7077")
                .setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("target/SparkTraining-1.0-SNAPSHOT.jar");

        ArrayList<Integer> randomArray = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            randomArray.add(random.nextInt());
        }

        JavaRDD<Integer> list = sc.parallelize(randomArray);
        JavaRDD<Integer> map = list.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return 10;
            }
        });
        System.out.println(list.count());
        System.out.println(map.take(10));
    }
}
