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

/**
 * @author nicola
 * @since 14/04/2017
 */
public class BasicCell implements Cell {

    private String name;

    private Class<?> type;

    private Object value;

    public BasicCell(String name, Class<?> type, Object value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Class<?> type() {
        return type;
    }

    @Override
    public Object value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BasicCell basicCell = (BasicCell) o;

        if (name != null ? !name.equals(basicCell.name) : basicCell.name != null) return false;
        if (type != null ? !type.equals(basicCell.type) : basicCell.type != null) return false;
        return value != null ? value.equals(basicCell.value) : basicCell.value == null;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BasicCell{");
        sb.append("name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }
}
