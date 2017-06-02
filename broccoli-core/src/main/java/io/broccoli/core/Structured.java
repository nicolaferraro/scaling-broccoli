package io.broccoli.core;

import javaslang.collection.List;

/**
 * @author nicola
 * @since 02/06/2017
 */
public interface Structured {

    List<String> names();

    List<Type> types();

    default String name(int pos) {
        return names().get(pos);
    }

    default int pos(String name) {
        return names().indexOf(name);
    }

    default Type type(int pos) {
        return types().get(pos);
    }

    default Type type(String name) {
        return type(pos(name));
    }

    default int size() {
        return types().size();
    }

}
