package org.stitz.quarkus_kafka_streams;

public class Movie {
    public int id;
    public String name;
    public String director;
    public String genre;

    public Movie(int id, String name, String director, String genre) {
        this.id = id;
        this.name = name;
        this.director = director;
        this.genre = genre;
    }

    @Override
    public String toString() {
        return "Movie{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", director='" + director + '\'' +
                ", genre='" + genre + '\'' +
                '}';
    }
}
