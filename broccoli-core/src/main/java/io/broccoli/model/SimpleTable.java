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

import io.broccoli.collection.SimpleVersionedMap;
import io.broccoli.collection.VersionedMap;
import io.broccoli.util.Arguments;

import javaslang.collection.List;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 13/04/2017
 */
public class SimpleTable<I extends Comparable<? super I>> implements Table<I> {

    private Schema schema;

    private VersionedMap<RowKey, Row, I> data;

    public SimpleTable(Schema schema, I zero) {
        this.schema = Arguments.requireNonNull(schema, "schema");
        this.data = new SimpleVersionedMap<>(Arguments.requireNonNull(zero, "zero"));
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public void addRow(Row row, I version) {
        RowKey key = new RowKey(schema, row);
        if (data.get(key, version).isDefined()) {
            throw new IllegalStateException("Duplicate key " + key);
        }

        data.put(key, row, version);
    }

    @Override
    public void deleteRow(Row row, I version) {
        RowKey key = new RowKey(schema, row);
        data.delete(key, version);
    }

    @Override
    public Flux<Row> rows(I version) {
        return data.streamValues(version);
    }

    static class RowKey {

        private Schema schema;
        private Row row;

        public RowKey(Schema schema, Row row) {
            this.schema = schema;
            this.row = row;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RowKey rowKey = (RowKey) o;

            return key().eq(rowKey.key());
        }

        @Override
        public int hashCode() {
            return key().hashCode();
        }

        private List<String> key() {
            List<String> key = List.empty();
            for (int i = 0; i < row.size(); i++) {
                if (!schema.isKey(i)) {
                    continue;
                }
                key = key.append(row.value(i));
            }
            return key;
        }

        @Override
        public String toString() {
            return key().toString();
        }
    }

}
