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
package io.broccoli.sql;

import io.broccoli.stream.Database;
import io.broccoli.stream.Table;
import io.broccoli.stream.basic.BasicQueryBuilder;
import io.broccoli.versioning.Version;

import javaslang.collection.List;

/**
 * @author nicola
 * @since 19/04/2017
 */
public class BroccoliSqlListener extends BroccoliBaseListener {

    private Database database;

    private Version version;

    private List<String> columns = List.empty();

    private List<String> tables = List.empty();

    public BroccoliSqlListener(Database database, Version version) {
        this.database = database;
        this.version = version;
    }

    @Override
    public void exitResultColumn(BroccoliParser.ResultColumnContext ctx) {
        columns = columns.append(ctx.getText());
    }

    @Override
    public void exitTableWithOptionalAlias(BroccoliParser.TableWithOptionalAliasContext ctx) {
        tables = tables.append(ctx.getText());
    }

    public Table build() {
        return new BasicQueryBuilder(database)
                .select(columns.toJavaArray(String.class))
                .from(tables.toJavaArray(String.class))
                .buildQuery(version);
    }

}
