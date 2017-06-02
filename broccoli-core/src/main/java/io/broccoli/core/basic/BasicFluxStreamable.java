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
import io.broccoli.core.Streamable;
import io.broccoli.core.Type;

import javaslang.collection.List;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class BasicFluxStreamable implements Streamable {

    private String name;

    private Flux<Event> flux;

    private List<String> names;

    private List<Type> types;

    public BasicFluxStreamable(String name, List<String> names, List<Type> types, Flux<Event> flux) {
        this.name = name;
        this.flux = flux;
        this.names = names;
        this.types = types;
    }

    @Override
    public List<String> names() {
        return this.names;
    }

    @Override
    public List<Type> types() {
        return this.types;
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
        return flux.cache(0); // always hot
    }

}
