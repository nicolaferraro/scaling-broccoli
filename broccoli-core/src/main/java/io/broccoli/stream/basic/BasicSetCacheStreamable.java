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
package io.broccoli.stream.basic;

import io.broccoli.collection.SimpleVersionedMap;
import io.broccoli.collection.VersionedMap;
import io.broccoli.stream.Event;
import io.broccoli.stream.Replayable;
import io.broccoli.stream.Row;
import io.broccoli.stream.Streamable;
import io.broccoli.stream.Table;
import io.broccoli.versioning.Version;
import io.broccoli.versioning.VersioningSystem;

import javaslang.control.Option;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class BasicSetCacheStreamable implements Streamable, Replayable, Table {

    private String name;

    private Streamable source;

    private volatile VersionedMap<Row, Row, Version> cache;

    public BasicSetCacheStreamable(String name, Streamable source, VersioningSystem versioningSystem) {
        this.name = name;
        this.source = source;
        this.cache = new SimpleVersionedMap<>(versioningSystem.zero());
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Flux<Row> stream(Version version) {
        return cache.streamValues(version);
    }

    @Override
    public Flux<Event> changes() {
        return source.changes()
                .map(e -> {
                    if (e.eventType() != Event.EventType.NOOP) {
                        Option<Row> existing = cache.get(e.row(), e.version());
                        if (e.eventType() == Event.EventType.ADD) {
                            if (existing.isEmpty()) {
                                cache.put(e.row(), e.row(), e.version());
                            } else {
                                return new BasicNoopEvent(e.version());
                            }
                        } else if (e.eventType() == Event.EventType.REMOVE) {
                            if (existing.isDefined()) {
                                cache.delete(e.row(), e.version());
                            } else {
                                return new BasicNoopEvent(e.version());
                            }
                        }
                    }
                    return e;
                }).cache(0);
    }
}
