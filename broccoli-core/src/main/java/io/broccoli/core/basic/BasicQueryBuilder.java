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
import io.broccoli.core.Query;
import io.broccoli.core.Streamable;
import io.broccoli.core.Table;
import io.broccoli.versioning.Version;

import javaslang.collection.List;

/**
 * @author nicola
 * @since 18/04/2017
 */
public class BasicQueryBuilder implements Query.Builder {

    private Database database;
    private List<String> tables;
    private List<String> columns;

    public BasicQueryBuilder(Database database) {
        this.database = database;
    }

    @Override
    public BasicFromClauseBuilder select(String... columns) {
        BasicQueryBuilder.this.columns = List.of(columns);
        return new BasicFromClauseBuilder();
    }

    public class BasicFromClauseBuilder implements Query.FromClauseBuilder {
        @Override
        public BasicWhereClauseBuilder from(String... tables) {
            BasicQueryBuilder.this.tables = List.of(tables);
            return new BasicWhereClauseBuilder();
        }
    }

    public class BasicWhereClauseBuilder extends BasicQueryFinalizerBuilder implements Query.WhereClauseBuilder {

    }

    public class BasicQueryFinalizerBuilder implements Query.QueryFinalizerBuilder {
        @Override
        public Table buildStructure() {

            List<Table> streams = tables.map(name -> database.table(name).get());
            BasicCartesianStreamable cartesian = new BasicCartesianStreamable("cartesian(" + tables.mkString(",") + ")", database.versioningSystem(), streams.toJavaArray(Table.class));
            BasicProjectionStreamable projection = new BasicProjectionStreamable("projection(" + cartesian.name() + ")", cartesian, database.versioningSystem(), columns.toJavaArray(String.class));

            return projection;
        }

        @Override
        public Table buildQuery(Version version) {

            List<Streamable> streams = tables.map(name -> database.table(name).get()).map(t -> new BasicTableReplayer(t.name(), t, version, database.versioningSystem()));
            BasicCartesianStreamable cartesian = new BasicCartesianStreamable("cartesian(" + tables.mkString(",") + ")", database.versioningSystem(), streams.toJavaArray(Streamable.class));
            BasicProjectionStreamable projection = new BasicProjectionStreamable("projection(" + cartesian.name() + ")", cartesian, database.versioningSystem(), columns.toJavaArray(String.class));

            return projection;
        }
    }
}
