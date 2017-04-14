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
package io.broccoli.operator;

import io.broccoli.model.ResultSet;
import io.broccoli.model.Table;
import io.broccoli.query.Query;
import io.broccoli.util.Arguments;

import javaslang.collection.Map;

/**
 * @author nicola
 * @since 13/04/2017
 */
public class QueryOperator<I extends Comparable<? super I>> implements Operator {

    private Map<String, Table<I>> tables;
    private Query query;
    private I version;

    public QueryOperator(Map<String, Table<I>> tables, Query query, I version) {
        this.tables = Arguments.requireNonNull(tables, "tables");
        this.query = Arguments.requireNonNull(query, "query");
        this.version = Arguments.requireNonNull(version, "version");
    }

    @Override
    public ResultSet apply() {
        Table<I> table = tables.get(query.getTable()).getOrElseThrow(() -> new IllegalArgumentException("Unknown table " + query.getTable()));
        Operator op = new FullTableOperator<I>(table, version);
        for (String field : query.getFieldEq().keySet()) {
            String value = query.getFieldEq().get(field).get();
            op = new FilterOperator(op.apply(), r -> value.equals(r.value(table.schema().columnPos(field).get())));
        }
        if (query.getFields().length != 1 && !query.getFields()[0].equals("*")) {
            op = new ProjectionOperator(op.apply(), query.getFields());
        }

        return op.apply();
    }
}
