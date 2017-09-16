package org.driver;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Created by pedromorfeu on 18/06/2017.
 */
public class DriverFilesPartitioning {

    public static void main(String[] args) throws IOException {
//        String master = "local[*]";
        String master = "spark://10.211.55.101:7077";
        SparkConf conf = new SparkConf()
                .setMaster(master)
                .setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("target/SparkTraining-1.0-SNAPSHOT.jar");

        JavaRDD<String> rdd = sc.parallelize(
                Arrays.asList(
                        "/vagrant/raspberry/Notes.txt",
                        "/vagrant/raspberry/run-hadoop.sh",
                        "/vagrant/raspberry/run-yarn.sh",
                        "/vagrant/raspberry/run-spark.sh"), 3);
//        rdd = rdd.repartition(2);
        System.out.println(rdd.getNumPartitions());

        String path = "target/out_part";
        deleteFolder(Paths.get(path.toString(), "_temporary").toString());
        deleteFolder(path);

        System.out.println(rdd.collect());
        rdd.saveAsTextFile(path);

        // Results for master local[C cores]:
        // sc.parallelize(N array) --> C partitions (N output files with data)
        // sc.parallelize(N array, M slices) --> M partitions (M output files with data)
        // rdd = sc.parallelize(N array); rdd.repartition(M) --> M partitions (1 output file with data)

        JavaRDD<String> map = rdd.map(s -> InetAddress.getLocalHost().toString() + "_" + Thread.currentThread().getId());
        System.out.println("Thread: " + Thread.currentThread().getId());
        System.out.println(map.collect());

        // Results for master cluster[3 machines, 2 cores each]:
        // sc.parallelize(N array) --> 2 partitions (2 IPs, 1 thread each) --> 2 threads
        // sc.parallelize(N array, M slices) --> M partitions (M IPs, 1 thread each) --> M threads
        // rdd = sc.parallelize(N array); rdd.repartition(M) --> M partitions (1 IP, M threads) --> M threads
    }

    private static void deleteFolder(String pathString) throws IOException {
        Path path = Paths.get(pathString);
        if(Files.exists(path)) {
            Files.list(path).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            Files.delete(path);
        }
    }
}
