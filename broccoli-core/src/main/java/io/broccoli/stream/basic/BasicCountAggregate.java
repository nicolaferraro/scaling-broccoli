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
package io.broccoli.stream.basic;

import io.broccoli.stream.Cell;
import io.broccoli.stream.Row;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class BasicCountAggregate implements Aggregate<Long> {

    private String name;

    private long count;

    public BasicCountAggregate(String name) {
        this(name, 0);
    }

    public BasicCountAggregate(String name, long count) {
        this.name = name;
        this.count = count;
    }

    @Override
    public Aggregate<Long> add(Row row) {
        return new BasicCountAggregate(name, count + 1);
    }

    @Override
    public Aggregate<Long> remove(Row row) {
        return new BasicCountAggregate(name, count - 1);
    }

    @Override
    public Cell<Long> get() {
        return new BasicCell<>(name, Long.class, count);
    }

    @Override
    public long supportingRows() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BasicCountAggregate that = (BasicCountAggregate) o;

        if (count != that.count) return false;
        return name != null ? name.equals(that.name) : that.name == null;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (int) (count ^ (count >>> 32));
        return result;
    }
}
