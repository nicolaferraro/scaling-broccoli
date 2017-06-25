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

import io.broccoli.core.Event;
import io.broccoli.core.Row;
import io.broccoli.core.Streamable;
import io.broccoli.core.Table;
import io.broccoli.core.Type;
import io.broccoli.versioning.Version;
import io.broccoli.versioning.VersioningSystem;

import javaslang.collection.List;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 18/04/2017
 */
public class BasicTableReplayer implements Table {

    private String name;
    private Table table;
    private Version version;
    private VersioningSystem v;

    public BasicTableReplayer(String name, Table table, Version version, VersioningSystem v) {
        this.name = name;
        this.table = table;
        this.version = version;
        this.v = v;
    }

    @Override
    public Flux<Row> stream(Version version) {
        return table.stream(version);
    }

    @Override
    public List<String> names() {
        return table.names();
    }

    @Override
    public List<Type> types() {
        return table.types();
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
        return table.stream(version).map(r -> new BasicEvent(r, Event.EventType.ADD, v.newSubVersion(version)));
    }
}
