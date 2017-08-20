package org.driver.function;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by pedromorfeu on 01/08/2017.
 */
public class WordCounter implements Function<String, Tuple2<Integer, String>> {

    @Override
    public Tuple2<Integer, String> call(String s) throws Exception {
        List<String> lines = Files.readAllLines(Paths.get(s));
        int result = lines.size();
        String ip = InetAddress.getLocalHost().toString();
        int result1 = Integer.parseInt(ip.substring(ip.length() - 3));
        return new Tuple2<>(result, InetAddress.getLocalHost().toString());
    }
}
