package org.stitz.quarkus_kafka_streams;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

@ApplicationScoped
public class TopologyProducer {

    private static final String MOVIES_TOPIC = "movies";
    private static final String PLAY_MOVIES_TOPIC = "playtimemovies";
    public static final String COUNT_MOVIE_STORE = "countMovieStore";

    KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(COUNT_MOVIE_STORE);

    @Produces
    public Topology getTopCharts() {

        final StreamsBuilder builder = new StreamsBuilder();

        final ObjectMapperSerde<Movie> movieSerder = new ObjectMapperSerde<>(Movie.class);
        final ObjectMapperSerde<PlayedMovie> moviePlayedSerder = new ObjectMapperSerde<>(PlayedMovie.class);
        final ObjectMapperSerde<MoviePlayCount> moviePlayCountSerder = new ObjectMapperSerde<>(MoviePlayCount.class);

        // Creation of a Global Kafka Table for Movies topic
        final GlobalKTable<Integer, Movie> moviesTable = builder.globalTable(
                MOVIES_TOPIC,
                Consumed.with(Serdes.Integer(), movieSerder));

        // Stream connected to playtimemovies topic, every event produced there is consumed by this stream
        final KStream<String, PlayedMovie> playEvents = builder.stream(
                PLAY_MOVIES_TOPIC, Consumed.with(Serdes.String(), moviePlayedSerder));

        // PlayedMovies has the region as key, and the object as value. Let’s map the content so the key is the movie id (to do the join) and leave the object as value
        // Moreover, we do the join using the keys of the movies table (movieId) and the keys of the stream (we changed it to be the movieId too in the map method).

        // Finally, the result is streamed to console
        playEvents
                .filter((region, event) -> event.duration >= 10) // filters by duration
                .map((key, value) -> KeyValue.pair(value.id, value)) // Now key is the id field
                .join(moviesTable, (movieId, moviePlayedId) -> movieId, (moviePlayed, movie) -> movie)
                .groupByKey(Grouped.with(Serdes.Integer(), movieSerder)) // Group events per key, in this case movie id
                // Aggregate method gets the MoviePlayCount object if already created (if not it creates it)
                // and calls its increment method to increment the view counter
                .aggregate(MoviePlayCount::new,
                        (movieId, movie, moviePlayCounter) -> moviePlayCounter.increment(movie.name),
                        Materialized.<Integer, MoviePlayCount> as(storeSupplier)
                                .withKeySerde(Serdes.Integer())
                                .withValueSerde(moviePlayCountSerder)
                ).toStream()
                .print(Printed.toSysOut());
        return builder.build();
    }
}
