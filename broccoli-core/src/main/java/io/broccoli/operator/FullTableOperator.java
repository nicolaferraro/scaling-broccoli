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
import io.broccoli.model.Row;
import io.broccoli.model.Schema;
import io.broccoli.model.Table;
import io.broccoli.util.Arguments;

import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 13/04/2017
 */
public class FullTableOperator<I extends Comparable<? super I>> implements Operator {

    private Table<I> table;

    private I version;

    public FullTableOperator(Table<I> table, I version) {
        this.table = Arguments.requireNonNull(table, "table");
        this.version = Arguments.requireNonNull(version, "version");
    }

    @Override
    public ResultSet apply() {
        return new ResultSet() {
            @Override
            public Schema schema() {
                return table.schema();
            }

            @Override
            public Flux<Row> rows() {
                return table.rows(version);
            }
        };
    }

}
