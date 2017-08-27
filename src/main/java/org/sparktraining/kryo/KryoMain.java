package org.sparktraining.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.InputChunked;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.OutputChunked;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

/**
 * Created by pedromorfeu on 27/08/2017.
 */
public class KryoMain {
    public static void main(String[] args) throws IOException {
        Kryo kryo = new Kryo();

        Files.list(Paths.get("target/kryo")).map(path -> {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return path;
        }).collect(Collectors.toList());

        OutputChunked output = new OutputChunked(new FileOutputStream("target/kryo/file.bin"), 1024);
        for (int i = 0; i < 100000; i++) {
            Trip trip = new Trip(i % 100, 2, 3);
            kryo.writeObject(output, trip);
            output.endChunks();
        }
        output.close();

        long start = System.currentTimeMillis();
        int total = 0;
        InputChunked input = new InputChunked(new FileInputStream("target/kryo/file.bin"), 1024);
        for (int i = 0; i < 100000; i++) {
            Trip tripRead = kryo.readObject(input, Trip.class);
            total += tripRead.getDay();
            input.nextChunks();
        }
        input.close();
        System.out.println("total: " + total);
        System.out.println("took " + (System.currentTimeMillis() - start));
    }
}
