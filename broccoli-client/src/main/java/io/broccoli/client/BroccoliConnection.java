package io.broccoli.client;

import io.broccoli.client.data.Row;

import org.reactivestreams.Publisher;

/**
 * @author nicola
 * @since 26/06/2017
 */
public interface BroccoliConnection {

    Publisher<Row> query(String sql);

}
