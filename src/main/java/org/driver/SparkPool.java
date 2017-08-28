package org.driver;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple2$;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Created by pedromorfeu on 27/06/2017.
 */
public class SparkPool {

    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf()
//                .setMaster("spark://10.211.55.101:7077")
                .setMaster("local[4]")
                .setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);

        int i = 0;

        while (true && i < 10) {
            System.out.println("Waiting for input...");
            System.in.read();

            // Lambda Runnable
            int finalI = ++i;
            Runnable task1 = () -> {
                System.out.println(new Date() + " Task #" + finalI + " is running");

                int random = new Random().nextInt(5);

                String pathname = "/Users/pedromorfeu/IdeaProjects/SparkTraining/books";
                String file = new File(pathname).listFiles((dir, name) -> !name.startsWith("."))[random].getAbsolutePath();
                System.out.println(new Date() + " Task #" + finalI + " file " + file);

                JavaRDD<String> rdd = sc.textFile(file);
                JavaPairRDD<Object, Object> counts = rdd
                        .flatMap(s -> Arrays.asList(s.split("\\s+")).iterator())
                        .mapToPair(s -> new Tuple2(s, 1))
                        .reduceByKey((o, o2) -> (Integer) o + (Integer) o2);
                List<Tuple2<Object, Object>> take = counts.take(10);
                System.out.println(new Date() + " Task #" + finalI + " result: " + take);
                long count = rdd.count();
                System.out.println(new Date() + " Task #" + finalI + " file " + file + " result: " + count);

                File out = new File("/Users/pedromorfeu/IdeaProjects/SparkTraining/target/output/out.txt");
                out.getParentFile().mkdirs();
                String s = "" + new Date() + " Task #" + finalI + " file " + file + " count: " + count + " result: " + take + "\r\n";
                try {
                    Files.write(out.toPath(), s.getBytes(), StandardOpenOption.APPEND);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            };

            // start the thread
            new Thread(task1).start();
        }

    }
}
