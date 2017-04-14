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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.broccoli.stream.basic.Aggregate;
import io.broccoli.stream.basic.AggregateFactory;
import io.broccoli.stream.basic.BasicAggregateStreamable;
import io.broccoli.stream.basic.BasicCartesianStreamable;
import io.broccoli.stream.basic.BasicCountAggregate;
import io.broccoli.stream.basic.BasicEvent;
import io.broccoli.stream.basic.BasicFilterStreamable;
import io.broccoli.stream.basic.BasicFluxStreamable;
import io.broccoli.stream.basic.BasicLastStreamable;
import io.broccoli.stream.basic.BasicProjectionStreamable;
import io.broccoli.stream.basic.BasicRow;
import io.broccoli.stream.basic.BasicSetCacheStreamable;
import io.broccoli.util.TestStreamFactory;
import io.broccoli.versioning.BasicVersioningSystem;
import io.broccoli.versioning.VersioningSystem;

import org.junit.Before;
import org.junit.Test;

import javaslang.collection.List;
import javaslang.collection.Traversable;
import javaslang.control.Option;
import reactor.core.publisher.Flux;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class StreamableTest {

    private VersioningSystem v;

    @Before
    public void init() {
        v = new BasicVersioningSystem();
    }

    @Test
    public void testFluxStreamable() {
        Streamable source = new BasicFluxStreamable("source",
                Flux.just(
                        TestStreamFactory.add(v.next(), "1", "Hello", "World")
                ));


        Event evt = source.changes().single().block();
        assertNotNull(evt);
        assertEquals(v.get(1), evt.version());
        assertEquals(List.of("1", "Hello", "World"), evt.row().cells().map(Cell::value));
    }

    @Test
    public void testFilterStreamable() {
        Streamable source = new BasicFluxStreamable("source",
                Flux.just(
                        TestStreamFactory.add(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.add(v.next(), "2", "From", "Broccoli"),
                        TestStreamFactory.remove(v.next(), "2", "From", "Broccoli")
                ));

        Streamable filter = new BasicFilterStreamable("filter", r -> Integer.parseInt((String) r.cell(0).value()) % 2 == 0, source);

        List<Event> evts = List.ofAll(filter.changes().collectList().block());
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
        Streamable source = new BasicFluxStreamable("source",
                Flux.just(
                        TestStreamFactory.add(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.remove(v.next(), "1", "Hello", "World")
                ));

        Streamable projection = new BasicProjectionStreamable("projection", source, v, "s2", "s1");

        List<Event> evts = List.ofAll(projection.changes().collectList().block());
        assertNotNull(evts);
        assertEquals(2, evts.size());
        assertEquals(List.of("World", "Hello"), evts.get(0).row().cells().map(Cell::value));
        assertEquals(Event.EventType.ADD, evts.get(0).eventType());
        assertEquals(List.of("World", "Hello"), evts.get(1).row().cells().map(Cell::value));
        assertEquals(Event.EventType.REMOVE, evts.get(1).eventType());
    }

    @Test
    public void testProjectionStreamableFakeRemoval() {
        Streamable source = new BasicFluxStreamable("source",
                Flux.just(
                        TestStreamFactory.add(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.add(v.next(), "2", "Hello", "World"),
                        TestStreamFactory.remove(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.remove(v.next(), "2", "Hello", "World")
                ));

        Streamable projection = new BasicProjectionStreamable("projection", source, v, "s1", "s2");

        List<Event> evts = List.ofAll(projection.changes().collectList().block());
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
        Streamable source = new BasicFluxStreamable("source",
                Flux.just(
                        TestStreamFactory.add(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.remove(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.add(v.next(), "1", "Hello", "Broccoli")
                ));

        BasicSetCacheStreamable cache = new BasicSetCacheStreamable("cache", source, v);

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
        Streamable source = new BasicFluxStreamable("source",
                Flux.just(
                        TestStreamFactory.remove(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.add(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.add(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.add(v.next(), "1", "Hello", "World2"),
                        TestStreamFactory.remove(v.next(), "1", "Hello", "World")
                ));

        Streamable cache = new BasicSetCacheStreamable("cache", source, v);

        List<Event> evts = List.ofAll(cache.changes().collectList().block());
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
        Streamable source = new BasicFluxStreamable("source",
                Flux.just(
                        TestStreamFactory.add(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.add(v.next(), "2", "Hello", "Worlde"),
                        TestStreamFactory.add(v.next(), "3", "Hello", "Broccoli"),
                        TestStreamFactory.remove(v.next(), "3", "Hello", "Broccoli"),
                        TestStreamFactory.remove(v.next(), "2", "Hello", "Worlde"),
                        TestStreamFactory.remove(v.next(), "1", "Hello", "World")
                ));

        BasicLastStreamable<String> last = new BasicLastStreamable<>("last", source, v, r -> new BasicRow(List.of(r.cell("s1"))), r -> (String) r.cell("s0").value());

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

    @Test
    public void testCountStreamable() throws InterruptedException {
        Streamable source = new BasicFluxStreamable("source",
                Flux.just(
                        TestStreamFactory.add(v.next(), "1", "Hello", "World"),
                        TestStreamFactory.add(v.next(), "2", "Hello", "Worlde"),
                        TestStreamFactory.add(v.next(), "3", "Hello2", "Broccoli"),
                        TestStreamFactory.remove(v.next(), "3", "Hello2", "Broccoli"),
                        TestStreamFactory.remove(v.next(), "2", "Hello", "Worlde"),
                        TestStreamFactory.remove(v.next(), "1", "Hello", "World")
                ));

        BasicAggregateStreamable count = new BasicAggregateStreamable("count", source, v, r -> new BasicRow(List.of(r.cell("s1"))), new AggregateFactory<Long>() {
            @Override
            public Aggregate<Long> newAggregate() {
                return new BasicCountAggregate("count(*)");
            }
        });

        CountDownLatch latch = new CountDownLatch(1);
        count.changes()
                .doOnComplete(latch::countDown)
                .subscribe();

        latch.await(5, TimeUnit.SECONDS);

        List<Row> rows6 = List.ofAll(count.stream(v.get(6)).collectList().block());
        assertEquals(0, rows6.size());

        List<Row> rows5 = List.ofAll(count.stream(v.get(5)).collectList().block());
        assertEquals(1, rows5.size());
        List<Traversable<String>> table5 = rows5.map(r -> r.cells().map(c -> c.value().toString()));
        assertTrue(table5.contains(List.of("Hello", "1")));

        List<Row> rows4 = List.ofAll(count.stream(v.get(4)).collectList().block());
        assertEquals(1, rows4.size());
        List<Traversable<String>> table4 = rows4.map(r -> r.cells().map(c -> c.value().toString()));
        assertTrue(table4.contains(List.of("Hello", "2")));

        List<Row> rows3 = List.ofAll(count.stream(v.get(3)).collectList().block());
        assertEquals(2, rows3.size());
        List<Traversable<String>> table3 = rows3.map(r -> r.cells().map(c -> c.value().toString()));
        assertTrue(table3.contains(List.of("Hello", "2")));
        assertTrue(table3.contains(List.of("Hello2", "1")));

        List<Row> rows2 = List.ofAll(count.stream(v.get(2)).collectList().block());
        assertEquals(1, rows2.size());
        List<Traversable<String>> table2 = rows2.map(r -> r.cells().map(c -> c.value().toString()));
        assertTrue(table2.contains(List.of("Hello", "2")));

        List<Row> rows1 = List.ofAll(count.stream(v.get(1)).collectList().block());
        assertEquals(1, rows1.size());
        List<Traversable<String>> table1 = rows1.map(r -> r.cells().map(c -> c.value().toString()));
        assertTrue(table1.contains(List.of("Hello", "1")));

        List<Row> rows0 = List.ofAll(count.stream(v.get(0)).collectList().block());
        assertEquals(0, rows0.size());

    }

    @Test
    public void testCartesianStreamable() throws InterruptedException {
        Streamable source1 = new BasicFluxStreamable("source1",
                Flux.just(
                        TestStreamFactory.add(v.next(), "1", "A"),
                        TestStreamFactory.add(v.next(), "2", "B"),
                        TestStreamFactory.remove(v.next(), "2", "B")
                ));
        Streamable source2 = new BasicFluxStreamable("source2",
                Flux.just(
                        TestStreamFactory.add(v.next(), Option.of("r"), "--1", "--A"),
                        TestStreamFactory.add(v.next(), Option.of("r"), "--2", "--B"),
                        TestStreamFactory.remove(v.next(), Option.of("r"), "--2", "--B")
                ));

        BasicCartesianStreamable cartesian = new BasicCartesianStreamable("prod", v, source1, source2);

        BasicSetCacheStreamable cache = new BasicSetCacheStreamable("cache", cartesian, v);

        CountDownLatch latch = new CountDownLatch(1);
        cache.changes()
                .doOnComplete(latch::countDown)
                .subscribe();

        latch.await(5, TimeUnit.SECONDS);

        List<Row> rows6 = List.ofAll(cache.stream(v.get(6)).collectList().block());
        assertEquals(1, rows6.size());

        List<Row> rows5 = List.ofAll(cache.stream(v.get(5)).collectList().block());
        assertEquals(2, rows5.size());
    }

    @Test
    public void testCartesianStreamableEvents() throws InterruptedException {
        Streamable source1 = new BasicFluxStreamable("source1",
                Flux.just(
                        TestStreamFactory.add(v.next(), "1", "A"),
                        TestStreamFactory.add(v.next(), "2", "B"),
                        TestStreamFactory.remove(v.next(), "2", "B")
                ));
        Streamable source2 = new BasicFluxStreamable("source2",
                Flux.just(
                        TestStreamFactory.add(v.next(), Option.of("r"), "--1", "--A"),
                        TestStreamFactory.add(v.next(), Option.of("r"), "--2", "--B"),
                        TestStreamFactory.remove(v.next(), Option.of("r"), "--2", "--B")
                ));

        BasicCartesianStreamable cartesian = new BasicCartesianStreamable("prod", v, source1, source2);

        List<Event.EventType> events = List.ofAll(cartesian.changes()
                .filter(e -> e instanceof BasicEvent && e.eventType() != Event.EventType.NOOP)
                .map(Event::eventType)
                .collectList().block());

        assertEquals(List.of(Event.EventType.ADD, Event.EventType.ADD, Event.EventType.REMOVE), events);
    }

}
