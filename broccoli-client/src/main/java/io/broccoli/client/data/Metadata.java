package io.broccoli.client.data;

import javaslang.collection.List;

/**
 * @author nicola
 * @since 26/06/2017
 */
public interface Metadata {

    List<String> columnNames();

    String columnName(int position);

    List<Type> columnTypes();

    Type columnType();

}
