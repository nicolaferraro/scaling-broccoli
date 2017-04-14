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
package io.broccoli.collection;

import java.util.List;

import org.junit.Test;

import javaslang.control.Option;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author nicola
 * @since 13/04/2017
 */
public class SimpleVersionedMapTest {

    @Test
    public void testSimpleMap() {
        VersionedMap<String, String, Integer> coll = new SimpleVersionedMap<>(0);

        coll.put("hello", "world", 1);
        coll.put("hello", "xxx", 2);
        coll.put("hi", "man", 3);
        coll.delete("hello", 4);


        assertEquals(Option.of("world"), coll.get("hello", 1));
        assertEquals(Option.of("xxx"), coll.get("hello", 2));
        assertEquals(Option.of("xxx"), coll.get("hello", 3));
        assertEquals(Option.none(), coll.get("hello", 4));
        assertEquals(Option.none(), coll.get("hi", 1));
        assertEquals(Option.of("man"), coll.get("hi", 3));
        assertEquals(Option.of("man"), coll.get("hi", 7));

        List<String> values = coll.streamValues(3).collectList().block();
        assertTrue(values.contains("xxx"));
        assertTrue(values.contains("man"));
        assertEquals(2, values.size());
        assertEquals(4, coll.version().intValue());
    }

    @Test
    public void testEmptyValues() {
        VersionedMap<String, String, Integer> coll = new SimpleVersionedMap<>(0);
        assertEquals(Option.none(), coll.get("hello", 0));
        assertEquals(0, coll.version().intValue());
    }

    @Test
    public void testNullValues() {
        VersionedMap<String, String, Integer> coll = new SimpleVersionedMap<>(0);
        coll.put("hello", null, 1);
        coll.put("hello", "xxx", 2);
        coll.put("hi", "man", 3);
        coll.put("hi", null, 4);
        coll.delete("hello", 5);
        coll.delete("hi", 6);

        assertEquals(Option.none(), coll.get("hello", 0));
        assertEquals(Option.none(), coll.get("hello", 1));
        assertEquals(Option.of("xxx"), coll.get("hello", 2));
        assertEquals(Option.none(), coll.get("hello", 5));
        assertEquals(Option.none(), coll.get("hello", 6));
        assertEquals(Option.none(), coll.get("hi", 2));
        assertEquals(Option.of("man"), coll.get("hi", 3));
        assertEquals(Option.none(), coll.get("hi", 4));
        assertEquals(Option.none(), coll.get("hi", 5));
        assertEquals(Option.none(), coll.get("hi", 6));

        List<String> values = coll.streamValues(2).collectList().block();
        assertTrue(values.contains("xxx"));
        assertEquals(1, values.size());

        List<String> values2 = coll.streamValues(5).collectList().block();
        assertEquals(0, values2.size());
        assertEquals(6, coll.version().intValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalInsertion() {
        VersionedMap<String, String, Integer> coll = new SimpleVersionedMap<>(0);
        try {
            coll.put("hello", "world", 1);
            coll.put("hello", "world", 1);
        } finally {
            assertEquals(1, coll.version().intValue());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalDeletion() {
        VersionedMap<String, String, Integer> coll = new SimpleVersionedMap<>(0);
        try {
            coll.put("hello", "world", 1);
            coll.delete("hello", 1);
        } finally {
            assertEquals(1, coll.version().intValue());
        }
    }

    @Test
    public void testAllowedDeletion() {
        VersionedMap<String, String, Integer> coll = new SimpleVersionedMap<>(0);
        coll.delete("hello", 1);
        assertEquals(1, coll.version().intValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullKey() {
        VersionedMap<String, String, Integer> coll = new SimpleVersionedMap<>(0);
        try {
            coll.put(null, "world", 1);
        } finally {
            assertEquals(0, coll.version().intValue());
        }
    }

}
