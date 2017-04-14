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
package io.broccoli.stream.basic;

import io.broccoli.stream.Event;
import io.broccoli.stream.Row;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class BasicNoopEvent<I extends Comparable<? super I>> implements Event<I> {

    private I version;

    public BasicNoopEvent(I version) {
        this.version = version;
    }

    @Override
    public Row row() {
        return null;
    }

    @Override
    public EventType eventType() {
        return EventType.NOOP;
    }

    @Override
    public I version() {
        return version;
    }

}
