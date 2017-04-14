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

/**
 * @author nicola
 * @since 13/04/2017
 */
public class SimpleRow implements Row {

    private String[] data;

    public SimpleRow(String... data) {
        this.data = Arguments.requireNonNull(data, "data");
    }

    @Override
    public int size() {
        return data.length;
    }

    @Override
    public String value(int pos) {
        Arguments.requireRange(pos, 0, data.length - 1);
        return data[pos];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleRow simpleRow = (SimpleRow) o;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(data, simpleRow.data);

    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SimpleRow{");
        sb.append("data=").append(Arrays.toString(data));
        sb.append('}');
        return sb.toString();
    }
}
