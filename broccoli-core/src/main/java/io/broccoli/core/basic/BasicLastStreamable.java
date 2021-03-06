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

import java.util.function.Function;

import io.broccoli.collection.SimpleVersionedMap;
import io.broccoli.collection.VersionedMap;
import io.broccoli.core.Event;
import io.broccoli.core.Replayable;
import io.broccoli.core.Row;
import io.broccoli.core.Streamable;
import io.broccoli.core.Type;
import io.broccoli.versioning.Version;
import io.broccoli.versioning.VersioningSystem;

import javaslang.collection.List;
import javaslang.control.Option;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class BasicLastStreamable<U extends Comparable<? super U>> implements Streamable, Replayable {

    private String name;

    private Streamable source;

    private Function<? super Row, ? extends Row> groupBy;
    private Function<? super Row, U> orderBy;

    private VersioningSystem versioningSystem;

    private volatile VersionedMap<Row, List<Row>, Version> cache;

    public BasicLastStreamable(String name, Streamable source, VersioningSystem versioningSystem, Function<? super Row, ? extends Row> groupBy, Function<? super Row, U> orderBy) {
        this.name = name;
        this.source = source;
        this.versioningSystem = versioningSystem;
        this.groupBy = groupBy;
        this.orderBy = orderBy;
        this.cache = new SimpleVersionedMap<>(versioningSystem.zero());
    }

    @Override
    public List<String> names() {
        return source.names();
    }

    @Override
    public List<Type> types() {
        return source.types();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Flux<Row> stream(Version version) {
        return cache.streamValues(version)
                .flatMapIterable(l -> l.sortBy(orderBy).lastOption());
    }

    @Override
    public boolean monotonic() {
        return false;
    }

    @Override
    public Flux<Event> changes() {
        return source.changes().flatMapIterable(e -> {
            if (e.eventType() != Event.EventType.NOOP) {
                Row groupByKey = groupBy.apply(e.row());
                Option<List<Row>> previous = cache.get(groupByKey, e.version());

                if (e.eventType() == Event.EventType.ADD) {
                    if (previous.isEmpty() || previous.get().isEmpty()) {
                        cache.put(groupByKey, List.of(e.row()), e.version());
                        return List.of(e);
                    } else {
                        List<Row> completeList = previous.get().append(e.row());
                        Row previousBest = previous.get().sortBy(orderBy).last();
                        Row newBest = completeList.sortBy(orderBy).last();
                        cache.put(groupByKey, completeList, e.version());
                        if (previousBest.equals(newBest)) {
                            return List.of(new BasicNoopEvent(e.version()));
                        } else {
                            return List.of(
                                    new BasicEvent(previousBest, Event.EventType.REMOVE, versioningSystem.newSubVersion(e.version())),
                                    new BasicEvent(newBest, Event.EventType.ADD, versioningSystem.newSubVersion(e.version())),
                                    new BasicNoopEvent(e.version())
                            );
                        }
                    }
                } else {
                    List<Row> newCacheEntry = previous.get().remove(e.row());
                    Row previousBest = previous.get().sortBy(orderBy).last();
                    Option<Row> newBest = newCacheEntry.sortBy(orderBy).lastOption();
                    cache.put(groupByKey, newCacheEntry, e.version());
                    if (newBest.isDefined() && previousBest.equals(newBest.get())) {
                        return List.of(new BasicNoopEvent(e.version()));
                    } else if (newBest.isDefined()) {
                        return List.of(
                                new BasicEvent(previousBest, Event.EventType.REMOVE, versioningSystem.newSubVersion(e.version())),
                                new BasicEvent(newBest.get(), Event.EventType.ADD, versioningSystem.newSubVersion(e.version())),
                                new BasicNoopEvent(e.version())
                        );
                    } else {
                        return List.of(new BasicEvent(previousBest, Event.EventType.REMOVE, e.version()));
                    }
                }

            }
            return List.of(e);
        }).cache(0);
    }
}
