package org.sparktraining;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparktraining.kryo.Trip;

import java.util.Arrays;

/**
 * Created by pedromorfeu on 27/08/2017.
 */
public class Kryo {

    public static void main(String[] args) {
        String master = "local[8]";
//        String master = "spark://10.211.55.101:7077";
        SparkConf conf = new SparkConf()
                .setMaster(master)
                .setAppName("Simple Application")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrationRequired", "false");

        conf.registerKryoClasses(new Class[]{Trip.class});

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Trip> rdd = sc.parallelize(
                Arrays.asList(
                        new Trip(1, 2, 3),
                        new Trip(1, 2, 3),
                        new Trip(1, 2, 3),
                        new Trip(1, 2, 3),
                        new Trip(1, 2, 3)
                ));

        System.out.println(rdd.mapToDouble(t -> t.getDay()).sum());

    }

}
