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

import javaslang.collection.List;
import javaslang.collection.Traversable;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class BasicRow implements Row {

    private List<Cell<?>> cells;

    public BasicRow(List<Cell<?>> cells) {
        this.cells = cells;
    }

    @Override
    public int size() {
        return cells.size();
    }

    @Override
    public Traversable<Cell<?>> cells() {
        return cells;
    }

    @Override
    public Cell<?> cell(int pos) {
        return cells.get(pos);
    }

    @Override
    public Cell<?> cell(String name) {
        return cell(cells.indexWhere(c -> name.equals(c.name())));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BasicRow basicRow = (BasicRow) o;

        return cells != null ? cells.equals(basicRow.cells) : basicRow.cells == null;

    }

    @Override
    public int hashCode() {
        return cells != null ? cells.hashCode() : 0;
    }
}
