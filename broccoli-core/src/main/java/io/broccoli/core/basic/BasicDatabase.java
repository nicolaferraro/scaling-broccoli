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

import io.broccoli.core.Database;
import io.broccoli.core.Event;
import io.broccoli.core.Query;
import io.broccoli.core.Streamable;
import io.broccoli.core.Table;
import io.broccoli.versioning.Version;
import io.broccoli.versioning.VersioningSystem;

import javaslang.collection.HashMap;
import javaslang.collection.List;
import javaslang.collection.Map;
import javaslang.collection.Seq;
import javaslang.collection.Traversable;
import javaslang.control.Option;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class BasicDatabase implements Database {

    private VersioningSystem v;

    private Seq<Table> tables;

    private ReplayProcessor<Version> currentVersion;

    private boolean started;

    private BasicDatabase(VersioningSystem v, Seq<Table> tables) {
        this.v = v;
        this.tables = tables;
        this.currentVersion = ReplayProcessor.cacheLast();
        this.started = false;
    }

    @Override
    public Traversable<Table> tables() {
        return this.tables;
    }


    @Override
    public Option<Table> table(String name) {
        return tables().filter(t -> name.equals(t.name())).headOption();
    }

    @Override
    public VersioningSystem versioningSystem() {
        return v;
    }

    @Override
    public Query.Builder newQueryBuilder() {
        return new BasicQueryBuilder(this);
    }

    @Override
    public void start() {
        if (started) {
            throw new IllegalStateException("Already started");
        }

        List<Streamable> allStreams = List.ofAll(tables);
        int leafs = 1; // TODO make it dynamically computed
        Flux.fromIterable(allStreams).flatMap(Streamable::changes)
                .map(Event::version)
                .filter(v::isRawVersion)
                .groupBy(ver -> ver)
                .flatMap(vg ->
                        vg.buffer(leafs)
                                .filter(l -> l.size() == leafs)
                                .map(l -> l.get(0))
                ).subscribe(currentVersion);

        this.started = true;
    }

    @Override
    public Flux<Version> currentVersion() {
        return currentVersion;
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
        public Database.Builder sourceTable(Table table) {
            if (tables.keySet().contains(table.name())) {
                throw new IllegalArgumentException("Name already present: " + table.name());
            }
            return new Builder(v, tables.put(table.name(), table));
        }

        @Override
        public Database build() {
            if (tables.size() == 0) {
                throw new IllegalStateException("No tables defined");
            }
            return new BasicDatabase(v, tables.values());
        }
    }
}
