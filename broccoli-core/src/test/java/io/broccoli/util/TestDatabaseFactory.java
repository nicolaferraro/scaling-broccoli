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
package io.broccoli.util;


import io.broccoli.core.Event;
import io.broccoli.core.Streamable;
import io.broccoli.core.Table;
import io.broccoli.core.Type;
import io.broccoli.core.basic.BasicEvent;
import io.broccoli.core.basic.BasicFluxStreamable;
import io.broccoli.core.basic.BasicRow;
import io.broccoli.core.basic.BasicSetCacheStreamable;
import io.broccoli.versioning.VersioningSystem;

import org.junit.Assert;

import javaslang.collection.List;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 18/04/2017
 */
public final class TestDatabaseFactory {

    private TestDatabaseFactory() {
    }

    public static StreamBuilder stream() {
        return new StreamBuilder();
    }

    public static class StreamBuilder {

        private VersioningSystem v;

        private String name;

        private List<String> columns;

        private List<Type> types;

        private List<List<Object>> rows = List.empty();

        public StreamBuilder name(String name) {
            this.name = name;
            return this;
        }

        public StreamBuilder versioningSysten(VersioningSystem v) {
            this.v = v;
            return this;
        }

        public StreamBuilder columns(String... columns) {
            this.columns = List.of(columns);
            return this;
        }

        public StreamBuilder columns(Type... types) {
            this.types = List.of(types);
            return this;
        }

        public StreamBuilder withRow(Object... row) {
            this.rows = this.rows.append(List.of(row));
            return this;
        }

        public Streamable build() {
            Flux<Event> events = Flux.fromIterable(rows)
                    .map(BasicRow::new)
                    .map(r -> new BasicEvent(r, Event.EventType.ADD, v.next()));

            return new BasicFluxStreamable(name, columns, types, events);
        }

        public Table buildTable() {
            return new BasicSetCacheStreamable(name, build(), v);
        }

        public Table buildTableImmediately() {
            Table t = buildTable();
            t.changes().last().block();
            Assert.assertEquals(rows.size(), t.stream(v.current()).collectList().block().size());
            return t;
        }

    }


}
