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

import java.util.Objects;

import io.broccoli.core.Database;
import io.broccoli.core.Query;
import io.broccoli.sql.builder.QueryBuilder;
import io.broccoli.versioning.Version;

/**
 * @author nicola
 * @since 18/04/2017
 */
public class BasicQueryBuilder implements Query.Builder {

    private Database database;

    private String sql;

    private Version version;

    public BasicQueryBuilder(Database database) {
        this.database = database;
    }

    @Override
    public Query.Builder version(Version version) {
        this.version = version;
        return this;
    }

    @Override
    public Query.Builder query(String sql) {
        this.sql = sql;
        return this;
    }

    @Override
    public Query build() {
        Database database = Objects.requireNonNull(this.database, "database");
        String sql = Objects.requireNonNull(this.sql, "sql");

        Version version = this.version;
        if (version == null) {
            version = database.versioningSystem().current();
        }

        QueryBuilder builder = new QueryBuilder(database);
        return builder.build(sql, version);
    }
}
