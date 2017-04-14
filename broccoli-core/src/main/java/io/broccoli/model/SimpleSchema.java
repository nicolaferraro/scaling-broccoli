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
package io.broccoli.model;

import java.util.Arrays;

import io.broccoli.util.Arguments;

import javaslang.collection.HashSet;
import javaslang.collection.Set;
import javaslang.collection.Stream;
import javaslang.control.Option;

/**
 * @author nicola
 * @since 13/04/2017
 */
public class SimpleSchema implements Schema {

    private String[] columns;
    private Set<Integer> keys;

    public SimpleSchema(Set<Integer> keys, String... columns) {
        this.columns = Arguments.requireNonNull(columns, "columns");
        this.keys = Arguments.requireNonNull(keys, "keys");

        if (columns.length != Stream.of(columns).distinct().length()) {
            throw new IllegalArgumentException("Duplicate column names");
        }
    }

    @Override
    public int columnCount() {
        return columns.length;
    }

    @Override
    public String columnName(int pos) {
        Arguments.requireRange(pos, 0, columns.length - 1);
        return columns[pos];
    }

    @Override
    public Option<Integer> columnPos(String name) {
        Arguments.requireNonNull(name, "name");
        return Stream.of(columns).zipWithIndex().filter(ci -> name.equals(ci._1)).map(ci -> ci._2.intValue()).headOption();
    }

    @Override
    public boolean isKey(int pos) {
        return keys.contains(pos);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SimpleSchema{");
        sb.append("columns=").append(Arrays.toString(columns));
        sb.append('}');
        return sb.toString();
    }
}
