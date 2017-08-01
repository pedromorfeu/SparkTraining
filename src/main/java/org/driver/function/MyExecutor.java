package org.driver.function;

import org.apache.spark.api.java.function.Function;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by pedromorfeu on 01/08/2017.
 */
public class MyExecutor implements Function<String, String> {

    @Override
    public String call(String s) throws Exception {
        List<String> lines = Files.readAllLines(Paths.get("/home/pi/raspberry/Notes.txt"));
        s = "hi " + s + " " + InetAddress.getLocalHost() + " " + lines.get(0);
        return s;
    }
}
