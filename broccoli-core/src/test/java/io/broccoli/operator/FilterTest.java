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
import io.broccoli.model.Schema;
import io.broccoli.model.SimpleRow;
import io.broccoli.model.SimpleSchema;
import io.broccoli.model.SimpleTable;
import io.broccoli.model.Table;

import org.junit.Before;
import org.junit.Test;

import javaslang.collection.HashSet;
import javaslang.collection.List;

import static org.junit.Assert.assertEquals;

/**
 * @author nicola
 * @since 13/04/2017
 */
public class FilterTest {

    private Table<Integer> table;

    @Before
    public void init() {
        Schema schema = new SimpleSchema(HashSet.of(0), "id", "name", "value");
        table = new SimpleTable<>(schema, 0);

        table.addRow(new SimpleRow("1", "Nicola", "Ferraro"), 1);
        table.addRow(new SimpleRow("2", "Nicola", "Ferraro2"), 2);
        table.addRow(new SimpleRow("3", "Nicola", "Ferraro3"), 3);
    }

    @Test
    public void testProjection() {
        ResultSet rs = new FilterOperator(new FullTableOperator<>(table, 4).apply(), r -> r.value(2).contains("3")).apply();

        List<String> strs = List.ofAll(rs.rows().map(r -> r.value(2)).collectList().block());
        assertEquals(1, strs.size());
        assertEquals("Ferraro3", strs.get(0));
    }

}
