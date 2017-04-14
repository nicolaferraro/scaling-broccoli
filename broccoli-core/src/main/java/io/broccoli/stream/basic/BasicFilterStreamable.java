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

import java.util.function.Predicate;

import io.broccoli.stream.Event;
import io.broccoli.stream.Row;
import io.broccoli.stream.Streamable;

import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class BasicFilterStreamable<I extends Comparable<? super I>> implements Streamable<I> {

    private String name;

    private Predicate<Row> filter;

    private Streamable<I> source;

    public BasicFilterStreamable(String name, Predicate<Row> filter, Streamable<I> source) {
        this.name = name;
        this.filter = filter;
        this.source = source;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Flux<Event<I>> changes() {
        return source.changes()
                .map(e -> {
                    if (filter.test(e.row())) {
                        return e;
                    } else {
                        return new BasicNoopEvent<>(e.version());
                    }
                });
    }

}