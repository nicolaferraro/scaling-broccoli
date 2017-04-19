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
package io.broccoli.sql;

import java.time.Duration;

import io.broccoli.stream.Database;
import io.broccoli.stream.Table;
import io.broccoli.stream.basic.BasicDatabase;
import io.broccoli.util.StreamUtils;
import io.broccoli.util.TestDatabaseFactory;
import io.broccoli.versioning.BasicVersioningSystem;
import io.broccoli.versioning.VersioningSystem;

import org.junit.Before;
import org.junit.Test;

/**
 * @author nicola
 * @since 19/04/2017
 */
public class ParserTest {

    private VersioningSystem v;

    @Before
    public void init() {
        v = new BasicVersioningSystem();
    }


    @Test
    public void testSimpleDBQuery() throws InterruptedException {

        Table users = TestDatabaseFactory.stream()
                .versioningSysten(v)
                .name("users")
                .columns("id", "name")
                .withRow(1, "Hello")
                .withRow(2, "Nicola")
                .buildTable();

        Database db = new BasicDatabase.Builder(v)
                .sourceTable(users)
                .build();

        db.start();
        db.currentVersion().last().block(Duration.ofSeconds(5)); // wait for db full

        Table result = new BroccoliQueryParser(db, v.current()).parseQuery("select name from users");
        result.changes().last().block(Duration.ofSeconds(5)); // wait for stream completion

        Table expected = TestDatabaseFactory.stream()
                .versioningSysten(v)
                .name("result")
                .columns("name")
                .withRow("Hello")
                .withRow("Nicola")
                .buildTableImmediately();

        StreamUtils.assertEquals(expected, result, v.current());
    }

}
