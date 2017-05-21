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
import io.broccoli.versioning.Version;
import io.broccoli.versioning.VersioningSystem;

import javaslang.collection.List;
import javaslang.collection.Traversable;
import javaslang.control.Option;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class BasicAggregateStreamable implements Streamable, Replayable {

    private String name;

    private Streamable source;

    private Function<? super Row, ? extends Row> groupBy;
    private List<AggregateFactory<?>> aggregateFactories;

    private VersioningSystem versioningSystem;

    private volatile VersionedMap<Row, List<Aggregate<?>>, Version> cache;

    public BasicAggregateStreamable(String name, Streamable source, VersioningSystem versioningSystem, Function<? super Row, ? extends Row> groupBy, AggregateFactory<?>... aggregateFactories) {
        this.name = name;
        this.source = source;
        this.versioningSystem = versioningSystem;
        this.groupBy = groupBy;
        this.aggregateFactories = List.of(aggregateFactories);
        this.cache = new SimpleVersionedMap<>(versioningSystem.zero());
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Flux<Row> stream(Version version) {
        return cache.streamEntries(version)
                .filter(t -> t._2.exists(a -> a.supportingRows() > 0))
                .map(t -> append(t._1, t._2));
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
                Option<List<Aggregate<?>>> previous = cache.get(groupByKey, e.version());

                if (e.eventType() == Event.EventType.ADD) {
                    if (previous.isEmpty() || previous.get().isEmpty()) {
                        List<Aggregate<?>> aggregates = aggregateFactories.map(AggregateFactory::newAggregate).map(a -> a.add(e.row()));
                        cache.put(groupByKey, aggregates, e.version());
                        return List.of(new BasicEvent(append(groupByKey, aggregates), Event.EventType.ADD, e.version()));
                    } else {
                        List<Aggregate<?>> previousAggregates = previous.get();
                        List<Aggregate<?>> newAggregates = previousAggregates.map(a -> a.add(e.row()));
                        ;
                        cache.put(groupByKey, newAggregates, e.version());

                        if (previousAggregates.equals(newAggregates)) {
                            return List.of(new BasicNoopEvent(e.version()));
                        } else {
                            return List.of(
                                    new BasicEvent(append(groupByKey, previousAggregates), Event.EventType.REMOVE, versioningSystem.newSubVersion(e.version())),
                                    new BasicEvent(append(groupByKey, newAggregates), Event.EventType.ADD, versioningSystem.newSubVersion(e.version())),
                                    new BasicNoopEvent(e.version())
                            );
                        }
                    }
                } else {
                    List<Aggregate<?>> previousAggregates = previous.get();
                    List<Aggregate<?>> newAggregates = previousAggregates.map(a -> a.remove(e.row()));
                    cache.put(groupByKey, newAggregates, e.version());

                    if (previousAggregates.equals(newAggregates)) {
                        return List.of(new BasicNoopEvent(e.version()));
                    } else {
                        return List.of(
                                new BasicEvent(append(groupByKey, previousAggregates), Event.EventType.REMOVE, versioningSystem.newSubVersion(e.version())),
                                new BasicEvent(append(groupByKey, newAggregates), Event.EventType.ADD, versioningSystem.newSubVersion(e.version())),
                                new BasicNoopEvent(e.version())
                        );
                    }
                }
            }
            return List.of(e);
        }).cache(0);
    }

    private Row append(Row row, Traversable<Aggregate<?>> aggregates) {
        return new BasicRow(List.ofAll(row.cells()).appendAll(aggregates.map(Aggregate::get)));
    }

}
