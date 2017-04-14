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
package io.broccoli.stream;

import io.broccoli.stream.basic.BasicFilterStreamable;
import io.broccoli.stream.basic.BasicFluxStreamable;
import io.broccoli.util.TestStreamFactory;

import org.junit.Test;

import javaslang.collection.List;
import reactor.core.publisher.Flux;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class StreamableTest {

    @Test
    public void testFluxStreamable() {
        Streamable<Integer> source = new BasicFluxStreamable<>("source",
                Flux.just(
                        TestStreamFactory.add(0, "1", "Hello", "World")
                ));


        Event<Integer> evt = source.changes().single().block();
        assertNotNull(evt);
        assertEquals(0, evt.version().intValue());
        assertEquals(List.of("1", "Hello", "World"), evt.row().cells().map(Cell::value));
    }

    @Test
    public void testFilterStreamable() {
        Streamable<Integer> source = new BasicFluxStreamable<>("source",
                Flux.just(
                        TestStreamFactory.add(0, "1", "Hello", "World"),
                        TestStreamFactory.add(1, "2", "From", "Broccoli"),
                        TestStreamFactory.remove(2, "2", "From", "Broccoli")
                ));

        Streamable<Integer> filter = new BasicFilterStreamable<>("filter", r -> Integer.parseInt((String) r.cell(0).value()) % 2 == 0, source);

        List<Event<Integer>> evts = List.ofAll(filter.changes().collectList().block());
        assertNotNull(evts);
        assertEquals(2, evts.size());
        assertEquals(List.of("2", "From", "Broccoli"), evts.get(0).row().cells().map(Cell::value));
        assertEquals(Event.EventType.ADD, evts.get(0).eventType());
        assertEquals(List.of("2", "From", "Broccoli"), evts.get(1).row().cells().map(Cell::value));
        assertEquals(Event.EventType.REMOVE, evts.get(1).eventType());
    }

}
