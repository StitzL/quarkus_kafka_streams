package org.stitz.quarkus_kafka_streams;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;

import java.util.Optional;

@ApplicationScoped
public class InteractiveQueries {

    @Inject
    KafkaStreams streams;


    public Optional<MoviePlayCountData> getMoviePlayCountData(int id) {
        // gets the state store and get the movie count by movie id
        MoviePlayCount moviePlayCount = getMoviesPlayCount().get(id);
        // Wrap the result into MoviePlayCountData
        return Optional.ofNullable(new MoviePlayCountData(moviePlayCount.name, moviePlayCount.count));
    }


    // Gets the state store
    private ReadOnlyKeyValueStore<Integer, MoviePlayCount> getMoviesPlayCount() {
        while (true) {
            try {
                return streams.store(fromNameAndType(TopologyProducer.COUNT_MOVIE_STORE, QueryableStoreTypes.keyValueStore()));
            } catch (InvalidStateStoreException e) {
                // ignore, store not ready yet
            }
        }
    }

}
