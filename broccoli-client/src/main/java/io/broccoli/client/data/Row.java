package io.broccoli.client.data;

/**
 * @author nicola
 * @since 26/06/2017
 */
public interface Row {

    Metadata metadata();

    Object get(int position);

    default <T> T get(int position, Class<T> type) {
        return type.cast(get(position));
    }

}
