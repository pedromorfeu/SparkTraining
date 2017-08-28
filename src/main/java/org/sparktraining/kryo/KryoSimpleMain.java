package org.sparktraining.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.InputChunked;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.OutputChunked;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

/**
 * Created by pedromorfeu on 27/08/2017.
 */
public class KryoSimpleMain {

    public static final int NUM_ENTRIES = 100000;
    public static final int NUM_FILES = 2000;
    public static final String FOLDER = "target/kryo-simple";

    public static void main(String[] args) throws IOException {

//        System.out.println("Sleeping...");
//        System.in.read();
//        System.out.println("Start!");

        if(true) {
            prepareFolder(FOLDER);
            Kryo kryoWrite = new Kryo();
            kryoWrite.register(Trip.class);
            kryoWrite.setReferences(false);
            for (int j = 0; j < NUM_FILES; j++) {
                Output output = new Output(new BufferedOutputStream(new FileOutputStream(FOLDER + "/file" + j + ".bin")));
                for (int i = 0; i < NUM_ENTRIES; i++) {
                    Trip trip = new Trip(i % 100, 5, 128 + (i % 100));
                    kryoWrite.writeObject(output, trip);
                }
                output.close();
            }
        }

        long start = System.currentTimeMillis();
        long total = 0;
        long totalSeen = 0;
        Kryo kryoRead = new Kryo();
        kryoRead.register(Trip.class);
        kryoRead.setReferences(false);
        for (int j = 0; j < NUM_FILES; j++) {
            Input input = new Input(new BufferedInputStream(new FileInputStream(FOLDER + "/file" + j + ".bin")), 1024);
            for (int i = 0; i < NUM_ENTRIES; i++) {
                Trip tripRead = kryoRead.readObject(input, Trip.class);
                if (tripRead.getDay() > 30 && tripRead.getDay() < 80) {
                    total += tripRead.getDay();
                }
                totalSeen++;
            }
            input.close();
        }
        System.out.println("total: " + total);
        System.out.println("total seen: " + totalSeen);
        System.out.println("took " + (System.currentTimeMillis() - start));
    }

    private static void prepareFolder(String folderPath) throws IOException {
        if(!Files.exists(Paths.get(folderPath))) {
            Files.createDirectory(Paths.get(folderPath));
            return;
        }
        Files.list(Paths.get(folderPath)).map(path -> {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return path;
        }).collect(Collectors.toList());
    }
}
