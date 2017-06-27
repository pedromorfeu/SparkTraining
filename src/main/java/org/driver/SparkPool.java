package org.driver;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple2$;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

/**
 * Created by pedromorfeu on 27/06/2017.
 */
public class SparkPool {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
//                .setMaster("spark://10.211.55.101:7077")
                .setMaster("local[4]")
                .setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);


        for (int i = 0; i < 5; i++) {
            // Lambda Runnable
            int finalI = i;
            Runnable task1 = () -> {
                System.out.println(new Date() + " Task #" + finalI + " is running");

                String file = new File("/Users/pedromorfeu/IdeaProjects/SparkTraining/books").listFiles()[finalI].getAbsolutePath();
                System.out.println(new Date() + " Task #" + finalI + " file " + file);
                JavaRDD<String> rdd = sc.textFile(file);
                JavaPairRDD<Object, Object> counts = rdd
                        .flatMap(s -> Arrays.asList(s.split("\\s+")))
                        .mapToPair(s -> new Tuple2(s, 1))
                        .reduceByKey((o, o2) -> (Integer) o + (Integer) o2);
                System.out.println(new Date() + " Task #" + finalI + " result: " + counts.take(10));

                System.out.println(new Date() + " Task #" + finalI + " file " + file + " result: " + rdd.count());
            };

            // start the thread
            new Thread(task1).start();
        }

    }

}
