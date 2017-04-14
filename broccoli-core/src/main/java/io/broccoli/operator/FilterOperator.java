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

import java.util.function.Predicate;

import io.broccoli.model.ResultSet;
import io.broccoli.model.Row;
import io.broccoli.model.Schema;
import io.broccoli.util.Arguments;

import javaslang.collection.List;
import javaslang.control.Option;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 13/04/2017
 */
public class FilterOperator implements Operator {

    private ResultSet resultSet;

    Predicate<Row> filter;

    public FilterOperator(ResultSet resultSet, Predicate<Row> filter) {
        this.resultSet = Arguments.requireNonNull(resultSet, "resultSet");
        this.filter = Arguments.requireNonNull(filter, "filter");
    }

    @Override
    public ResultSet apply() {
        return new ResultSet() {
            @Override
            public Schema schema() {
                return resultSet.schema();
            }

            @Override
            public Flux<Row> rows() {
                return resultSet.rows()
                        .filter(filter);
            }
        };
    }
}
