package org.stitz.quarkus_kafka_streams;

public class PlayedMovie {
    public int id;
    public long duration;

    public PlayedMovie(int id, long duration) {
        this.id = id;
        this.duration = duration;
    }
}
