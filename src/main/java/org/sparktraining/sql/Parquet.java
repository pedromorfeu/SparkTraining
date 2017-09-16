package org.sparktraining.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by pedromorfeu on 07/09/2017.
 */
public class Parquet {

    public static void main(String[] args) {
        String master = "local[*]";
        SparkConf conf = new SparkConf()
                .setMaster(master)
                .setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);

//        SQLContext sqlContext = new SQLContext(sc);
//
//        JavaRDD<Person> map = sc.textFile("data/people.txt").map(s -> new Person(s.split(",")[0], Integer.parseInt(s.split(",")[1])));
//        System.out.println(map.take(2));
//
//        map.saveAsTextFile("target/people_" + System.currentTimeMillis());
//
//        Dataset<Row> dataFrame = sqlContext.createDataFrame(map, Person.class);
//
//        System.out.println("Schema:");
//        dataFrame.printSchema();
//
//        dataFrame.registerTempTable("people");
//
//        Dataset<Row> all = sqlContext.sql("select name, age from people");
//
//        System.out.println(all.javaRDD().map(v1 -> v1.getString(0) + "_" + v1.getInt(1)).collect());
//
//        String path = "target/people_parquet_" + System.currentTimeMillis();
//        all.write().parquet(path);
//
//        Dataset<Row> parquet = sqlContext.read().parquet(path);
//
//        System.out.println("Schema:");
//        parquet.printSchema();
//
//        parquet.registerTempTable("peopleRestored");
//
//        Dataset<Row> allRestored = sqlContext.sql("select name, age from peopleRestored");
//
//        System.out.println(allRestored.javaRDD().collect());
//
//        Dataset<Row> names = allRestored.where("age >= 30").where("age < 37").select("name");
//        System.out.println(names.javaRDD().toDebugString());
//        System.out.println(names.collect());
//        System.out.println(names.javaRDD().collect());

    }

}
