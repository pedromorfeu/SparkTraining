package org.sparktraining.kryo;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class JavaBinaryReadWrite {

    public static final int NUM_RECORDS = 100000;

    public static void main(String[] args) throws IOException, InterruptedException {
//        System.out.println(0b00000001 << 2);
//        System.out.println(0b00000100 >> 1);
//        System.out.println((byte) 100);
//        System.out.println(110 % 100);

//        System.out.println("Sleeping...");
//        System.in.read();
//        System.out.println("Start!");

        if (false) {
            Files.createDirectories(Paths.get("target/data/"));
            for (int j = 0; j < 2000; j++) {
                try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream("target/data/out" + j + ".data", false));) {
                    ByteArrayOutputStream buf = new ByteArrayOutputStream();
                    for (int i = 0; i < NUM_RECORDS; i++) {
                        int day = i % 100;
                        int stay = 5;
                        int city = 128 + (i % 100);
                        Trip trip = new Trip(day, stay, city);

                        write(buf, trip);
                    }
                    out.write(buf.toByteArray());
                }
            }
        }

        long start = System.currentTimeMillis();
        long total = 0;
        long totalSeen=0;
        for (int j = 0; j < 2000; j++) {
            try (BufferedInputStream in = new BufferedInputStream(new FileInputStream("target/data/out" + j + ".data"));) {
                for (int i = 0; i < NUM_RECORDS; i++) {
                    Trip trip = read(in);
                    if (trip.getDay() > 30 && trip.getDay() < 80) {
                        total += trip.day;
                    }
                    totalSeen++;
                }
            }
        }
        System.out.println(total);
        System.out.println("totalSeen: " + totalSeen);
        System.out.println("Took " + (System.currentTimeMillis() - start));

    }

    private static void write(ByteArrayOutputStream buf, Trip trip) {
        byte byteDay = (byte) trip.getDay();
        buf.write(byteDay);

        byte byteStay = (byte) trip.getStay();
        buf.write(byteStay);

        byte byte1 = (byte) (trip.getCity() >> 8);
        buf.write(byte1);
        byte byte2 = (byte) trip.getCity();
        buf.write(byte2);
    }

    private static Trip read(BufferedInputStream in) throws IOException {
        byte readDay = (byte) in.read();
        int day = (int) (readDay & 0xFF);

        byte readStay = (byte) in.read();
        short stay = (short) (readStay & 0xFF);

        byte read1 = (byte) in.read();
        byte read2 = (byte) in.read();
        int city = (int) ((read1 & 0xFF) << 8) | (read2 & 0xFF);

        return new Trip(day, stay, city);
    }
}
