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

import io.broccoli.stream.basic.BasicDatabase;
import io.broccoli.util.TestStreamFactory;
import io.broccoli.versioning.BasicVersioningSystem;
import io.broccoli.versioning.Version;
import io.broccoli.versioning.VersioningSystem;

import org.junit.Before;
import org.junit.Test;

import javaslang.control.Option;
import reactor.core.publisher.Flux;

import static org.junit.Assert.assertEquals;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class DatabaseTest {

    private VersioningSystem v;

    @Before
    public void init() {
        v = new BasicVersioningSystem();
    }

    @Test
    public void testDB() throws InterruptedException {

        Flux<Event> source1 = Flux.just(
                TestStreamFactory.add(v.next(), "1"),
                TestStreamFactory.add(v.next(), "2"),
                TestStreamFactory.remove(v.next(), "2")
        );

        Flux<Event> source2 = Flux.just(
                TestStreamFactory.add(v.next(), Option.of("r"), "A"),
                TestStreamFactory.add(v.next(), Option.of("r"), "B"),
                TestStreamFactory.add(v.next(), Option.of("r"), "C")
        );

        Database db = new BasicDatabase.Builder(v)
                .sourceTable("source1", source1)
                .sourceTable("source2", source2)
                .build();

        db.start();

        Version[] versions = new Version[1];
        CountDownLatch latch = new CountDownLatch(1);
        db.currentVersion()
                .doOnNext(version -> versions[0] = version)
                .doOnComplete(latch::countDown)
                .subscribe();

        assertEquals(2, db.tables().size());

        latch.await(5, TimeUnit.SECONDS);
        assertEquals(v.get(6), versions[0]);
    }

}
