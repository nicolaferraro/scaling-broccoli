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
package io.broccoli.core.basic;

import io.broccoli.core.Event;
import io.broccoli.core.Replayable;
import io.broccoli.core.Streamable;
import io.broccoli.versioning.Version;
import io.broccoli.versioning.VersioningSystem;

import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 18/04/2017
 */
public class BasicReplayableReplayer implements Streamable {

    private String name;
    private Replayable replayable;
    private Version version;
    private VersioningSystem v;

    public BasicReplayableReplayer(String name, Replayable replayable, Version version, VersioningSystem v) {
        this.name = name;
        this.replayable = replayable;
        this.version = version;
        this.v = v;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean monotonic() {
        return true;
    }

    @Override
    public Flux<Event> changes() {
        return replayable.stream(version).map(r -> new BasicEvent(r, Event.EventType.ADD, v.newSubVersion(version)));
    }
}
