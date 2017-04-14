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

import io.broccoli.stream.Event;
import io.broccoli.stream.Replayable;
import io.broccoli.stream.Row;
import io.broccoli.stream.Streamable;
import io.broccoli.versioning.Version;
import io.broccoli.versioning.VersioningSystem;

import javaslang.Tuple;
import javaslang.collection.List;
import javaslang.control.Option;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class BasicCartesianStreamable implements Streamable {

    private String name;
    private VersioningSystem versioningSystem;
    private List<Streamable> sources;

    public BasicCartesianStreamable(String name, VersioningSystem versioningSystem, Streamable... sources) {
        this.name = name;
        this.versioningSystem = versioningSystem;
        this.sources = List.of(sources).map(s -> {
            if (s instanceof Replayable) {
                return s;
            } else {
                return new BasicSetCacheStreamable(s.name(), s, versioningSystem);
            }
        });
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Flux<Event> changes() {
        return Flux.merge(sources.zipWithIndex().map(t -> t._1.changes().map(e -> Tuple.of(t._2, e))))
                .flatMap(t -> {
                    int source = t._1.intValue();
                    Event e = t._2;

                    if (e.eventType() != Event.EventType.NOOP) {
                        List<Replayable> rSources = sources.removeAt(source).map(s -> (Replayable) s).insert(source, new SingleRowReplayable(e.row()));
                        Replayable product = rSources.foldLeft(Option.<Replayable>none(), (s1, s2) -> Option.of(product(s1, s2, e.version()))).get();

                        long x = product.stream(e.version())
                                .<Event>map(r -> new BasicEvent(r, e.eventType(), versioningSystem.newSubVersion(e.version()))).count().block();

                        return product.stream(e.version())
                                .<Event>map(r -> new BasicEvent(r, e.eventType(), versioningSystem.newSubVersion(e.version())))
                                .concatWith(Flux.<Event>just(new BasicNoopEvent(e.version())));
                    }

                    return Flux.just(e);
                });
    }

    private Replayable product(Option<Replayable> s1, Replayable s2, Version version) {
        if (s1.isEmpty()) {
            return s2;
        }
        return v -> s1.get().stream(version).flatMap(r1 -> s2.stream(version).map(r2 -> concat(r1, r2)));
    }

    private Row concat(Row r1, Row r2) {
        return new BasicRow(List.ofAll(r1.cells()).appendAll(r2.cells()));
    }

}
