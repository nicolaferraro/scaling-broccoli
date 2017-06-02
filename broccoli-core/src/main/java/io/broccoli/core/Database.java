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
package io.broccoli.core;

import io.broccoli.versioning.Version;
import io.broccoli.versioning.VersioningSystem;

import javaslang.collection.Traversable;
import javaslang.control.Option;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 14/04/2017
 */
public interface Database {

    Traversable<Table> tables();

    Option<Table> table(String name);

    VersioningSystem versioningSystem();

    Query.Builder newQueryBuilder();

    void start();

    Flux<Version> currentVersion();

    interface Builder {

        Builder sourceTable(Table table);

        Database build();

    }

}
