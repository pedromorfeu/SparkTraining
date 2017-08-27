package org.sparktraining.kryo;

/**
 * Created by pedromorfeu on 27/08/2017.
 */
public class Trip {
    int day;
    int stay;
    int city;

    public Trip() {
        // empty constructor
    }

    public Trip(int day, int stay, int city) {
        this.day = day;
        this.stay = stay;
        this.city = city;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getStay() {
        return stay;
    }

    public void setStay(int stay) {
        this.stay = stay;
    }

    public int getCity() {
        return city;
    }

    public void setCity(int city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return "Trip{" +
                "day=" + day +
                ", stay=" + stay +
                ", city=" + city +
                '}';
    }
}
