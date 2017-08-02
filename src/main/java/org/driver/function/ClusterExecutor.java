package org.driver.function;

import org.apache.spark.api.java.function.Function;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by pedromorfeu on 01/08/2017.
 */
public class ClusterExecutor implements Function<String, String> {

    @Override
    public String call(String s) throws Exception {
        String path = null;
        switch (s) {
            case "one":
                path = "/home/pi/raspberry/Notes.txt";
                break;
            case "two":
                path = "/home/pi/raspberry/format-hadoop.sh";
                break;
        }

        List<String> lines = Files.readAllLines(Paths.get(path));
        s = "hi " + s + " " + InetAddress.getLocalHost() + " text: " + lines.get(0);
        return s;
    }
}
