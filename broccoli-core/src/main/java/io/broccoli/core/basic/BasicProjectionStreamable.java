/*
 * Copyright 2016 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package io.broccoli.core.basic;

import io.broccoli.collection.SimpleVersionedMap;
import io.broccoli.collection.VersionedMap;
import io.broccoli.core.Event;
import io.broccoli.core.Replayable;
import io.broccoli.core.Row;
import io.broccoli.core.Streamable;
import io.broccoli.core.Table;
import io.broccoli.versioning.Version;
import io.broccoli.versioning.VersioningSystem;

import javaslang.collection.List;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class BasicProjectionStreamable implements Streamable, Replayable, Table {

    private String name;

    private Streamable source;

    private List<String> columns;

    private volatile VersionedMap<Row, Long, Version> cache;

    public BasicProjectionStreamable(String name, Streamable source, VersioningSystem versioningSystem, String... columns) {
        this.name = name;
        this.source = source;
        this.columns = List.of(columns);
        this.cache = new SimpleVersionedMap<>(versioningSystem.zero());
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Flux<Row> stream(Version version) {
        return cache.streamEntries(version)
                .filter(t -> t._2.longValue() > 0)
                .map(t -> t._1);
    }

    @Override
    public boolean monotonic() {
        return true;
    }

    @Override
    public Flux<Event> changes() {
        return source.changes()
                .map(e -> {
                    switch (e.eventType()) {
                    case NOOP:
                        return e;
                    case ADD:
                    case REMOVE:
                        Row key = projection(e.row());
                        long count = cache.get(key, e.version()).getOrElse(0L);
                        long newCount = e.eventType() == Event.EventType.ADD ? count + 1 : count - 1;
                        if (newCount < 0) {
                            throw new IllegalStateException("Deletion of inexistent row");
                        }
                        cache.put(key, newCount, e.version());
                        if (count == 0 && newCount > count) {
                            return new BasicEvent(key, Event.EventType.ADD, e.version());
                        } else if (count > 0 && newCount == 0) {
                            return new BasicEvent(key, Event.EventType.REMOVE, e.version());
                        } else {
                            return new BasicNoopEvent(e.version());
                        }
                    default:
                        throw new IllegalArgumentException("Unknown event type " + e);
                    }
                }).cache(0);
    }

    private Row projection(Row original) {
        return new BasicRow(columns.map(original::cell));
    }

}
