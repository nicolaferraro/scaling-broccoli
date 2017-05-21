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
package io.broccoli.core;

import java.time.Duration;

import io.broccoli.core.basic.BasicDatabase;
import io.broccoli.util.StreamUtils;
import io.broccoli.util.TestDatabaseFactory;
import io.broccoli.versioning.BasicVersioningSystem;
import io.broccoli.versioning.VersioningSystem;

import org.junit.Before;
import org.junit.Test;

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
    public void testDBProjection() throws InterruptedException {

        Table s = TestDatabaseFactory.stream()
                .versioningSysten(v)
                .name("s")
                .columns("s1", "s2")
                .withRow("a", 1)
                .withRow("a", 2)
                .buildTable();

        Database db = new BasicDatabase.Builder(v)
                .sourceTable(s)
                .build();

        db.start();
        db.currentVersion().last().block(Duration.ofSeconds(5)); // wait for db full

        Table result = db.newQueryBuilder()
                .select("s1")
                .from("s")
                .buildQuery(v.current());
        result.changes().last().block(Duration.ofSeconds(5)); // wait for stream completion

        Table expected = TestDatabaseFactory.stream()
                .versioningSysten(v)
                .name("result")
                .columns("s1")
                .withRow("a")
                .buildTableImmediately();

        StreamUtils.assertEquals(expected, result, v.current());
    }

    @Test
    public void testDBCartesian() throws InterruptedException {

        Table s = TestDatabaseFactory.stream()
                .versioningSysten(v)
                .name("s")
                .columns("s1", "s2")
                .withRow("a", 1)
                .withRow("a", 2)
                .buildTable();

        Table r = TestDatabaseFactory.stream()
                .versioningSysten(v)
                .name("r")
                .columns("r1")
                .withRow("x")
                .withRow("z")
                .buildTable();

        Database db = new BasicDatabase.Builder(v)
                .sourceTable(s)
                .sourceTable(r)
                .build();

        db.start();
        db.currentVersion().last().block(Duration.ofSeconds(5)); // wait for db full


        Table result = db.newQueryBuilder()
                .select("s2", "r1")
                .from("s", "r")
                .buildQuery(v.current());
        result.changes().last().block(Duration.ofSeconds(5)); // wait for stream completion


        Table expected = TestDatabaseFactory.stream()
                .versioningSysten(v)
                .name("result")
                .columns("s2", "r1")
                .withRow(1, "x")
                .withRow(1, "z")
                .withRow(2, "x")
                .withRow(2, "z")
                .buildTableImmediately();

        StreamUtils.assertEquals(expected, result, v.current());
    }

}
