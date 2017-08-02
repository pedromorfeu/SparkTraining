package org.driver;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.driver.function.Executor;

import java.util.Arrays;
import java.util.List;

/**
 * Created by pedromorfeu on 18/06/2017.
 */
public class DriverFiles {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[8]")
                .set("spark.driver.memory", "512m")
                .set("spark.worker.memory", "512m")
                .set("spark.executor.memory", "512m")
                .set("spark.task.cpus", "4")
                .setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("three", "four"));
        List<String[]> collect = rdd.map(new Executor()).collect();
        for (String[] list : collect) {
            for(int i=0;i<list.length;i++){
                System.out.println(list[i]);
            }
        }
    }
}
