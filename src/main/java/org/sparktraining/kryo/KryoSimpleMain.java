package org.sparktraining.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.InputChunked;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.OutputChunked;

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

        if(false) {
            prepareFolder(FOLDER);
            Kryo kryoWrite = new Kryo();
            for (int j = 0; j < NUM_FILES; j++) {
                Output output = new Output(new FileOutputStream(FOLDER + "/file" + j + ".bin"));
                for (int i = 0; i < NUM_ENTRIES; i++) {
                    Trip trip = new Trip(i % 100, 2, 3);
                    kryoWrite.writeObject(output, trip);
                }
                output.close();
            }
        }

        long start = System.currentTimeMillis();
        long total = 0;
        for (int j = 0; j < NUM_FILES; j++) {
            Input input = new Input(new FileInputStream(FOLDER + "/file" + j + ".bin"));
            Kryo kryoRead = new Kryo();
            for (int i = 0; i < NUM_ENTRIES; i++) {
                Trip tripRead = kryoRead.readObject(input, Trip.class);
                total += tripRead.getDay();
            }
            input.close();
        }
        System.out.println("total: " + total);
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
