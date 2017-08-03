package org.driver.function;

import org.apache.spark.api.java.function.Function;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by pedromorfeu on 01/08/2017.
 */
public class Executor implements Function<String, String[]> {

    @Override
    public String[] call(String s) throws Exception {
        List<String> lines = Files.readAllLines(Paths.get(s));
        String[] res = new String[]{
                "1 hi " + s + " " + InetAddress.getLocalHost() + " text: " + lines.get(0),
                "2 hi " + s + " " + InetAddress.getLocalHost() + " text: " + lines.get(0)};
        return res;
    }
}
