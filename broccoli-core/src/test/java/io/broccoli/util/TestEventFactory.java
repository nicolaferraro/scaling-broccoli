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

import io.broccoli.core.Event;
import io.broccoli.core.basic.BasicCell;
import io.broccoli.core.basic.BasicEvent;
import io.broccoli.core.basic.BasicRow;
import io.broccoli.versioning.Version;

import javaslang.collection.List;
import javaslang.control.Option;

/**
 * @author nicola
 * @since 14/04/2017
 */
public final class TestEventFactory {

    private TestEventFactory() {
    }

    public static Event add(Version version, String... data) {
        return add(version, Option.none(), data);
    }

    public static Event add(Version version, Option<String> prefix, String... data) {
        int[] idx = new int[]{0};
        return new BasicEvent(new BasicRow(List.of(data).map(o -> new BasicCell(prefix.getOrElse("s") + (idx[0]++), String.class, o))), Event.EventType.ADD, version);
    }

    public static Event remove(Version version, String... data) {
        return remove(version, Option.none(), data);
    }

    public static Event remove(Version version, Option<String> prefix, String... data) {
        int[] idx = new int[]{0};
        return new BasicEvent(new BasicRow(List.of(data).map(o -> new BasicCell(prefix.getOrElse("s") + (idx[0]++), String.class, o))), Event.EventType.REMOVE, version);
    }

}
