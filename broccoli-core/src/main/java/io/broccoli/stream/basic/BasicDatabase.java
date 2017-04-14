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

import io.broccoli.stream.Database;
import io.broccoli.stream.Event;
import io.broccoli.stream.Table;
import io.broccoli.versioning.VersioningSystem;

import javaslang.collection.HashMap;
import javaslang.collection.Map;
import javaslang.collection.Seq;
import javaslang.collection.Traversable;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class BasicDatabase implements Database {

    private Seq<Table> tables;

    private BasicDatabase(Seq<Table> tables) {
        this.tables = tables;
    }

    @Override
    public Traversable<Table> tables() {
        return this.tables;
    }

    public static class Builder implements Database.Builder {

        private VersioningSystem v;

        private Map<String, Table> tables;

        public Builder(VersioningSystem v) {
            this(v, HashMap.empty());
        }

        private Builder(VersioningSystem v, Map<String, Table> tables) {
            this.v = v;
            this.tables = tables;
        }

        @Override
        public Database.Builder sourceTable(String name, Flux<Event> stream) {
            if (tables.keySet().contains(name)) {
                throw new IllegalArgumentException("Name already present: " + name);
            }
            return new Builder(v, tables.put(name, new BasicSetCacheStreamable(name, new BasicFluxStreamable("source-of-" + name, stream), v)));
        }

        @Override
        public Database build() {
            return new BasicDatabase(tables.values());
        }
    }
}
