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

import io.broccoli.stream.Event;
import io.broccoli.stream.basic.BasicCell;
import io.broccoli.stream.basic.BasicEvent;
import io.broccoli.stream.basic.BasicRow;

import javaslang.collection.List;

/**
 * @author nicola
 * @since 14/04/2017
 */
public final class TestStreamFactory {

    private TestStreamFactory() {
    }

    public static <I extends Comparable<? super I>> Event<I> add(I version, String... data) {
        int[] idx = new int[]{1};
        return new BasicEvent<I>(new BasicRow(List.of(data).map(o -> new BasicCell<String>("s" + (idx[0]++), String.class, o))), Event.EventType.ADD, version);
    }

    public static <I extends Comparable<? super I>> Event<I> remove(I version, String... data) {
        int[] idx = new int[]{1};
        return new BasicEvent<I>(new BasicRow(List.of(data).map(o -> new BasicCell<String>("s" + (idx[0]++), String.class, o))), Event.EventType.REMOVE, version);
    }

}
