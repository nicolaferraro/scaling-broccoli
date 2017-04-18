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
package io.broccoli.util;


import io.broccoli.stream.Replayable;
import io.broccoli.stream.Row;
import io.broccoli.stream.Streamable;
import io.broccoli.stream.basic.BasicSetCacheStreamable;
import io.broccoli.versioning.Version;
import io.broccoli.versioning.VersioningSystem;

import org.junit.Assert;

import javaslang.collection.HashSet;
import javaslang.collection.List;
import reactor.core.publisher.Flux;

import static org.junit.Assert.assertTrue;

/**
 * @author nicola
 * @since 18/04/2017
 */
public final class StreamUtils {

    private StreamUtils() {
    }

    public static void assertEquals(Replayable r1, Replayable r2, Version version) {
        assertEquals(r1.stream(version), r2.stream(version));
    }

    public static void assertEquals(Flux<Row> r1, Flux<Row> r2) {
        List<Row> rows1 = List.ofAll(r1.collectList().block());
        List<Row> rows2 = List.ofAll(r2.collectList().block());
        Assert.assertEquals(rows1.size(), rows2.size());
        Assert.assertEquals(HashSet.ofAll(rows1), HashSet.ofAll(rows2));
    }

}
