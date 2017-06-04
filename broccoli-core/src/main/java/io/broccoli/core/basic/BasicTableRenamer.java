package io.broccoli.core.basic;

import io.broccoli.core.Event;
import io.broccoli.core.Replayable;
import io.broccoli.core.Row;
import io.broccoli.core.Streamable;
import io.broccoli.core.Table;
import io.broccoli.core.Type;
import io.broccoli.versioning.Version;

import javaslang.collection.List;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 04/06/2017
 */
public class BasicTableRenamer implements Streamable, Replayable, Table {

    private Table table;

    private String prefix;

    public BasicTableRenamer(Table table, String name) {
        this.table = table;
        this.prefix = name;
    }

    @Override
    public List<String> names() {
        return table.names().map(n -> prefix + "." + n);
    }

    @Override
    public List<Type> types() {
        return table.types();
    }

    @Override
    public String name() {
        return this.prefix;
    }

    @Override
    public Flux<Event> changes() {
        return table.changes();
    }

    @Override
    public Flux<Row> stream(Version version) {
        return table.stream(version);
    }

    @Override
    public boolean monotonic() {
        return table.monotonic();
    }
}
