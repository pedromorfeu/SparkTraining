package org.driver;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.driver.function.ClusterExecutor;

import java.util.Arrays;
import java.util.List;

/**
 * Created by pedromorfeu on 18/06/2017.
 */
public class DriverClusterFiles {

    public static void main(String[] args) {
//        String master = "spark://192.168.0.171:7077";
        String master = "spark://10.211.55.101:7077";
        SparkConf conf = new SparkConf()
                .setMaster(master)
//                .set("spark.driver.memory", "512m")
//                .set("spark.worker.memory", "512m")
//                .set("spark.executor.memory", "512m")
                .set("spark.task.cpus", "2")
                .setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("target/SparkTraining-1.0-SNAPSHOT.jar");

//        JavaRDD<String> rdd = sc.textFile("file:///home/pi/raspberry/Notes.txt");
//        JavaPairRDD<String, Integer> reducedRDD = rdd.flatMap(s -> Arrays.asList(s.split("\\s+")))
//                .mapToPair(s -> new Tuple2<>(s, 1))
//                .reduceByKey((a, b) -> a + b);
//        System.out.println(reducedRDD.take(10));

        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("/home/pi/raspberry/Notes.txt", "/home/pi/raspberry/run-hadoop.sh"));
//        rdd = rdd.repartition(2);
        List<String> collect = rdd.map(new ClusterExecutor()).collect();
        System.out.println(collect);
    }
}
