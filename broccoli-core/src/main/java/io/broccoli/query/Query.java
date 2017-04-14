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
package io.broccoli.query;

import io.broccoli.model.ResultSet;
import io.broccoli.model.Table;
import io.broccoli.operator.QueryOperator;
import io.broccoli.util.Arguments;

import javaslang.collection.HashMap;
import javaslang.collection.Map;

/**
 * @author nicola
 * @since 13/04/2017
 */
public class Query {

    private String[] fields;
    private String table;
    private Map<String, String> fieldEq;

    private Query() {
    }

    public <I extends Comparable<? super I>> ResultSet execute(Map<String, Table<I>> tables, I version) {
        return new QueryOperator<>(tables, this, version).apply();
    }

    public String[] getFields() {
        return fields;
    }

    public String getTable() {
        return table;
    }

    public Map<String, String> getFieldEq() {
        return fieldEq;
    }

    public static QueryTableSelectionBuilder select(String... fields) {
        Query query = new Query();
        query.fields = Arguments.requireNonNull(fields, "fields");
        if (query.fields.length == 0) {
            throw new IllegalArgumentException("Select at least one field");
        }
        return query.newQueryTableSelectionBuilder();
    }

    private QueryTableSelectionBuilder newQueryTableSelectionBuilder() {
        return new QueryTableSelectionBuilder();
    }

    public class QueryTableSelectionBuilder {

        public QueryConditionBuilder from(String table) {
            Query.this.table = Arguments.requireNonNull(table, "table");
            return new QueryConditionBuilder();
        }

    }

    public class QueryConditionBuilder {

        public QueryConditionBuilder whereFieldEq(String field, String value) {
            if (Query.this.fieldEq == null) {
                Query.this.fieldEq = HashMap.empty();
            }
            Query.this.fieldEq = Query.this.fieldEq.put(field, value);
            return this;
        }

        public Query end() {
            return Query.this;
        }

    }

}
