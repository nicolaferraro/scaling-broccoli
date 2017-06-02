package io.broccoli.core.basic;

import io.broccoli.core.Row;
import io.broccoli.core.TableEvent;
import io.broccoli.versioning.Version;

/**
 * @author nicola
 * @since 02/06/2017
 */
public class BasicTableEvent extends BasicEvent implements TableEvent {

    private String table;

    public BasicTableEvent(String table, Row row, EventType eventType, Version version) {
        super(row, eventType, version);

        this.table = table;
    }

    @Override
    public String table() {
        return table;
    }
}
