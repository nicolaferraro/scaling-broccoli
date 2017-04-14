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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.broccoli.stream.basic.BasicFilterStreamable;
import io.broccoli.stream.basic.BasicFluxStreamable;
import io.broccoli.stream.basic.BasicLastStreamable;
import io.broccoli.stream.basic.BasicProjectionStreamable;
import io.broccoli.stream.basic.BasicRow;
import io.broccoli.stream.basic.BasicSetCacheStreamable;
import io.broccoli.util.TestStreamFactory;
import io.broccoli.versioning.BasicVersioningSystem;
import io.broccoli.versioning.VersioningSystem;

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
        assertEquals(3, evts.size());
        assertEquals(Event.EventType.NOOP, evts.get(0).eventType());
        assertEquals(List.of("2", "From", "Broccoli"), evts.get(1).row().cells().map(Cell::value));
        assertEquals(Event.EventType.ADD, evts.get(1).eventType());
        assertEquals(List.of("2", "From", "Broccoli"), evts.get(2).row().cells().map(Cell::value));
        assertEquals(Event.EventType.REMOVE, evts.get(2).eventType());
    }

    @Test
    public void testProjectionStreamable() {
        BasicVersioningSystem v = new BasicVersioningSystem();

        Streamable<String> source = new BasicFluxStreamable<>("source",
                Flux.just(
                        TestStreamFactory.add(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.remove(v.next(), "1", "Hello", "World")
                ));

        Streamable<String> projection = new BasicProjectionStreamable<>("projection", source, v, "s2", "s1");

        List<Event<String>> evts = List.ofAll(projection.changes().collectList().block());
        assertNotNull(evts);
        assertEquals(2, evts.size());
        assertEquals(List.of("World", "Hello"), evts.get(0).row().cells().map(Cell::value));
        assertEquals(Event.EventType.ADD, evts.get(0).eventType());
        assertEquals(List.of("World", "Hello"), evts.get(1).row().cells().map(Cell::value));
        assertEquals(Event.EventType.REMOVE, evts.get(1).eventType());
    }

    @Test
    public void testProjectionStreamableFakeRemoval() {
        BasicVersioningSystem v = new BasicVersioningSystem();
        Streamable<String> source = new BasicFluxStreamable<>("source",
                Flux.just(
                        TestStreamFactory.add(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.add(v.next(), "2", "Hello", "World"),
                        TestStreamFactory.remove(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.remove(v.next(), "2", "Hello", "World")
                ));

        Streamable<String> projection = new BasicProjectionStreamable<>("projection", source, v, "s1", "s2");

        List<Event<String>> evts = List.ofAll(projection.changes().collectList().block());
        assertNotNull(evts);
        assertEquals(4, evts.size());
        assertEquals(Event.EventType.ADD, evts.get(0).eventType());
        assertEquals(List.of("Hello", "World"), evts.get(0).row().cells().map(Cell::value));
        assertEquals(Event.EventType.NOOP, evts.get(1).eventType());
        assertEquals(Event.EventType.NOOP, evts.get(2).eventType());
        assertEquals(Event.EventType.REMOVE, evts.get(3).eventType());
        assertEquals(List.of("Hello", "World"), evts.get(3).row().cells().map(Cell::value));
    }

    @Test
    public void testSetCacheStreamable() throws InterruptedException {
        BasicVersioningSystem v = new BasicVersioningSystem();
        Streamable<String> source = new BasicFluxStreamable<>("source",
                Flux.just(
                        TestStreamFactory.add(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.remove(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.add(v.next(), "1", "Hello", "Broccoli")
                ));

        BasicSetCacheStreamable<String> cache = new BasicSetCacheStreamable<>("cache", source, v);

        CountDownLatch latch = new CountDownLatch(1);
        cache.changes()
                .doOnComplete(latch::countDown)
                .subscribe();

        latch.await(5, TimeUnit.SECONDS);

        List<Row> rows2 = List.ofAll(cache.stream(v.get(3)).collectList().block());
        assertEquals(List.of("1", "Hello", "Broccoli"), rows2.get(0).cells().map(Cell::value));

        List<Row> rows1 = List.ofAll(cache.stream(v.get(2)).collectList().block());
        assertEquals(List.empty(), rows1);

        List<Row> rows0 = List.ofAll(cache.stream(v.get(1)).collectList().block());
        assertEquals(List.of("1", "Hello", "World"), rows0.get(0).cells().map(Cell::value));
    }

    @Test
    public void testSetCacheStreamableEvents() throws InterruptedException {
        BasicVersioningSystem v = new BasicVersioningSystem();
        Streamable<String> source = new BasicFluxStreamable<>("source",
                Flux.just(
                        TestStreamFactory.remove(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.add(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.add(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.add(v.next(), "1", "Hello", "World2"),
                        TestStreamFactory.remove(v.next(), "1", "Hello", "World")
                ));

        Streamable<String> cache = new BasicSetCacheStreamable<>("cache", source, v);

        List<Event<String>> evts = List.ofAll(cache.changes().collectList().block());
        assertNotNull(evts);
        assertEquals(5, evts.size());

        assertEquals(Event.EventType.NOOP, evts.get(0).eventType());
        assertEquals(v.get(1), evts.get(0).version());

        assertEquals(List.of("1", "Hello", "World"), evts.get(1).row().cells().map(Cell::value));
        assertEquals(Event.EventType.ADD, evts.get(1).eventType());
        assertEquals(v.get(2), evts.get(1).version());

        assertEquals(Event.EventType.NOOP, evts.get(2).eventType());
        assertEquals(v.get(3), evts.get(2).version());

        assertEquals(List.of("1", "Hello", "World2"), evts.get(3).row().cells().map(Cell::value));
        assertEquals(Event.EventType.ADD, evts.get(3).eventType());
        assertEquals(v.get(4), evts.get(3).version());

        assertEquals(List.of("1", "Hello", "World"), evts.get(4).row().cells().map(Cell::value));
        assertEquals(Event.EventType.REMOVE, evts.get(4).eventType());
        assertEquals(v.get(5), evts.get(4).version());
    }

    @Test
    public void testLastStreamable() throws InterruptedException {
        BasicVersioningSystem v = new BasicVersioningSystem();
        Streamable<String> source = new BasicFluxStreamable<>("source",
                Flux.just(
                        TestStreamFactory.add(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.add(v.next(), "2", "Hello", "Worlde"),
                        TestStreamFactory.add(v.next(), "3", "Hello", "Broccoli"),
                        TestStreamFactory.remove(v.next(), "3", "Hello", "Broccoli"),
                        TestStreamFactory.remove(v.next(), "2", "Hello", "Worlde"),
                        TestStreamFactory.remove(v.next(), "1", "Hello", "World")
                ));

        BasicLastStreamable<String, String> last = new BasicLastStreamable<>("last", source, v, r -> new BasicRow(List.of(r.cell("s1"))), r -> (String) r.cell("s0").value());

        CountDownLatch latch = new CountDownLatch(1);
        last.changes()
                .doOnComplete(latch::countDown)
                .subscribe();

        latch.await(5, TimeUnit.SECONDS);

        List<Row> rows6 = List.ofAll(last.stream(v.get(6)).collectList().block());
        assertEquals(0, rows6.size());

        List<Row> rows5 = List.ofAll(last.stream(v.get(5)).collectList().block());
        assertEquals(1, rows5.size());
        assertEquals(List.of("1", "Hello", "World"), rows5.get(0).cells().map(Cell::value));

        List<Row> rows4 = List.ofAll(last.stream(v.get(4)).collectList().block());
        assertEquals(1, rows4.size());
        assertEquals(List.of("2", "Hello", "Worlde"), rows4.get(0).cells().map(Cell::value));

        List<Row> rows3 = List.ofAll(last.stream(v.get(3)).collectList().block());
        assertEquals(1, rows3.size());
        assertEquals(List.of("3", "Hello", "Broccoli"), rows3.get(0).cells().map(Cell::value));

        List<Row> rows2 = List.ofAll(last.stream(v.get(2)).collectList().block());
        assertEquals(1, rows2.size());
        assertEquals(List.of("2", "Hello", "Worlde"), rows2.get(0).cells().map(Cell::value));

        List<Row> rows1 = List.ofAll(last.stream(v.get(1)).collectList().block());
        assertEquals(1, rows1.size());
        assertEquals(List.of("1", "Hello", "World"), rows1.get(0).cells().map(Cell::value));

        List<Row> rows0 = List.ofAll(last.stream(v.get(0)).collectList().block());
        assertEquals(0, rows0.size());

    }

}
