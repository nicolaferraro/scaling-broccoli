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
package io.broccoli.core.basic;

import java.util.Objects;

import io.broccoli.core.Row;

import javaslang.collection.List;
import javaslang.collection.Seq;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class BasicRow implements Row {

    private List<Object> cells;

    public BasicRow(List<Object> cells) {
        this.cells = Objects.requireNonNull(cells, "cells cannot be null");
    }

    @Override
    public Seq<Object> cells() {
        return this.cells;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BasicRow basicRow = (BasicRow) o;

        return cells.equals(basicRow.cells);

    }

    @Override
    public int hashCode() {
        return cells.hashCode();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BasicRow{");
        sb.append("cells=").append(cells);
        sb.append('}');
        return sb.toString();
    }
}
