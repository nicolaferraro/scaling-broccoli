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

import org.junit.Test;

import javaslang.collection.HashSet;

import static org.junit.Assert.assertEquals;

/**
 * @author nicola
 * @since 13/04/2017
 */
public class SimpleTableTest {

    @Test
    public void testSimpleTable() {

        Schema schema = new SimpleSchema(HashSet.of(0), "id", "name");
        Table<Integer> table = new SimpleTable<>(schema, 0);

        table.addRow(new SimpleRow("1", "ciccio"), 1);
        table.addRow(new SimpleRow("2", "ciccio"), 2);

        String val = table.rows(2)
                .map(r -> r.value(1))
                .distinct()
                .single()
                .block();

        assertEquals("ciccio", val);
    }

    @Test(expected = IllegalStateException.class)
    public void testDuplicateKey() {
        Schema schema = new SimpleSchema(HashSet.of(0), "id", "name");
        Table<Integer> table = new SimpleTable<>(schema, 0);

        table.addRow(new SimpleRow("1", "ciccio"), 1);
        table.addRow(new SimpleRow("1", "cola"), 2);
    }

}
