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
import io.broccoli.versioning.Version;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class BasicEvent implements Event {

    private Row row;
    private EventType eventType;
    private Version version;

    public BasicEvent(Row row, EventType eventType, Version version) {
        this.row = row;
        this.eventType = eventType;
        this.version = version;
    }

    @Override
    public Row row() {
        return row;
    }

    @Override
    public EventType eventType() {
        return eventType;
    }

    @Override
    public Version version() {
        return version;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BasicEvent{");
        sb.append("row=").append(row);
        sb.append(", eventType=").append(eventType);
        sb.append(", version=").append(version);
        sb.append('}');
        return sb.toString();
    }
}
