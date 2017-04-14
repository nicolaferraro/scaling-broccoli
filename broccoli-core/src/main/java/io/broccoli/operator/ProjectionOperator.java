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
import io.broccoli.util.Arguments;

import javaslang.collection.List;
import javaslang.control.Option;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 13/04/2017
 */
public class ProjectionOperator implements Operator {

    private ResultSet resultSet;

    private List<Integer> positions;

    public ProjectionOperator(ResultSet resultSet, String... columns) {
        this.resultSet = Arguments.requireNonNull(resultSet, "resultSet");
        Arguments.requireNonNull(columns, "columns");

        this.positions = List.of(columns).map(resultSet.schema()::columnPos).map(c -> c.getOrElseThrow(() -> new IllegalArgumentException("Unknown column name")));
    }

    @Override
    public ResultSet apply() {
        return new ResultSet() {
            @Override
            public Schema schema() {
                return new Schema() {
                    @Override
                    public int columnCount() {
                        return positions.size();
                    }

                    @Override
                    public String columnName(int pos) {
                        return resultSet.schema().columnName(positions.get(pos));
                    }

                    @Override
                    public Option<Integer> columnPos(String name) {
                        return resultSet.schema().columnPos(name).map(positions::indexOf);
                    }

                    @Override
                    public boolean isKey(int pos) {
                        return false;
                    }
                };
            }

            @Override
            public Flux<Row> rows() {
                return resultSet.rows()
                        .map(r -> new Row() {
                            @Override
                            public int size() {
                                return positions.size();
                            }

                            @Override
                            public String value(int pos) {
                                return r.value(positions.get(pos));
                            }
                        });
            }
        };
    }
}
