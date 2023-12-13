package org.acme.kafka.producer;

import java.util.UUID;

import io.smallrye.mutiny.Multi;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.acme.kafka.model.Quote;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;


@Path("/quotes")
public class QuotesResource {

    @Channel("quotes")
    Multi<Quote> quotes;
    @Channel("quote-requests")
    Emitter<String> quoteRequestEmitter;

    /**
     * Endpoint to generate a new quote request id and send it to "quote-requests" Kafka topic using the emitter.
     */
    @POST
    @Path("/request")
    @Produces(MediaType.TEXT_PLAIN)
    public String createRequest() {
        UUID uuid = UUID.randomUUID();
        quoteRequestEmitter.send(uuid.toString());
        return uuid.toString();
    }

    @GET
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public Multi<Quote> stream() {
        return quotes;
    }
}