package org.stitz.quarkus_kafka_streams;

import java.util.Optional;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

@Path("/moviecount")
public class MovieCountResource {

    // Injects the previous class to make queries
    @Inject
    InteractiveQueries interactiveQueries;

    @GET
    @Path("/data/{id}")
    public Response movieCountData(@PathParam("id") int id) {
        Optional<MoviePlayCountData> moviePlayCountData = interactiveQueries.getMoviePlayCountData(id);

        // Depending on the result returns the value or a 404
        if (moviePlayCountData.isPresent()) {
            return Response.ok(moviePlayCountData.get()).build();
        } else {
            return Response.status(Status.NOT_FOUND.getStatusCode(),
                    "No data found for movie " + id).build();
        }

    }
}
