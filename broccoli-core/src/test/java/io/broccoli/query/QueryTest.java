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
package io.broccoli.query;

import io.broccoli.model.ResultSet;
import io.broccoli.model.Schema;
import io.broccoli.model.SimpleRow;
import io.broccoli.model.SimpleSchema;
import io.broccoli.model.SimpleTable;
import io.broccoli.model.Table;

import org.junit.Before;
import org.junit.Test;

import javaslang.Tuple;
import javaslang.collection.HashMap;
import javaslang.collection.HashSet;
import javaslang.collection.List;
import javaslang.collection.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author nicola
 * @since 13/04/2017
 */
public class QueryTest {

    private Map<String, Table<Integer>> tables;

    @Before
    public void init() {
        Schema schema = new SimpleSchema(HashSet.of(0), "id", "name", "value");

        Table<Integer> table1 = new SimpleTable<>(schema, 0);
        table1.addRow(new SimpleRow("1", "Nicola", "Ferraro"), 1);
        table1.addRow(new SimpleRow("2", "Nicola", "Ferraro2"), 2);
        table1.addRow(new SimpleRow("3", "Nicola", "Ferraro3"), 3);

        Table<Integer> table2 = new SimpleTable<>(schema, 0);
        table2.addRow(new SimpleRow("4", "A", "X"), 1);
        table2.addRow(new SimpleRow("5", "B", "X"), 2);
        table2.addRow(new SimpleRow("6", "C", "X"), 3);
        table2.deleteRow(new SimpleRow("6", "C", "X"), 4);

        tables = HashMap.of("table1", table1, "table2", table2);
    }

    @Test
    public void testQuery() {
        ResultSet rs = Query.select("value", "name").from("table1").whereFieldEq("id", "3").end().execute(tables, 5);
        assertEquals(Tuple.of("Ferraro3", "Nicola"), rs.rows().map(r -> Tuple.of(r.value(0), r.value(1))).single().block());
    }

    @Test
    public void testMultiQuery() {
        ResultSet rs = Query.select("value", "name").from("table2").whereFieldEq("value", "X").end().execute(tables, 3);
        assertEquals(List.of("A", "B", "C"), List.ofAll(rs.rows().map(r -> r.value(1)).collectList().block()));
    }

    @Test
    public void testMultiOldQuery() {
        ResultSet rs = Query.select("value", "name").from("table2").whereFieldEq("value", "X").end().execute(tables, 2);
        assertEquals(List.of("A", "B"), List.ofAll(rs.rows().map(r -> r.value(1)).collectList().block()));
    }

    @Test
    public void testMultiDeleteQuery() {
        ResultSet rs = Query.select("value", "name").from("table2").whereFieldEq("value", "X").end().execute(tables, 4);
        assertEquals(List.of("A", "B"), List.ofAll(rs.rows().map(r -> r.value(1)).collectList().block()));
    }

}
