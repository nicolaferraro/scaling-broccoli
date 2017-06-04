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

import java.util.function.Predicate;

import io.broccoli.core.Event;
import io.broccoli.core.Row;
import io.broccoli.core.Streamable;
import io.broccoli.core.Type;

import javaslang.collection.List;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class BasicFilterStreamable implements Streamable {

    private String name;

    private Predicate<Row> filter;

    private Streamable source;

    public BasicFilterStreamable(String name, Predicate<Row> filter, Streamable source) {
        this.name = name;
        this.filter = filter;
        this.source = source;
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
    public boolean monotonic() {
        return true;
    }

    @Override
    public Flux<Event> changes() {
        return source.changes()
                .map(e -> {
                    if (e.eventType() != Event.EventType.NOOP) {
                        if (filter.test(e.row())) {
                            return e;
                        } else {
                            return new BasicNoopEvent(e.version());
                        }
                    }
                    return e;
                });
    }

}
