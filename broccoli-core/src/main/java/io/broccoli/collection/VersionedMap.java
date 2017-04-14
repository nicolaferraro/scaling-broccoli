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
package io.broccoli.collection;

import javaslang.Tuple2;
import javaslang.control.Option;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 12/04/2017
 */
public interface VersionedMap<K, V, I extends Comparable<? super I>> {

    void put(K key, V value, I version);

    Option<V> get(K key, I version);

    void delete(K key, I version);

    I version();

    Flux<K> streamKeys(I version);

    Flux<V> streamValues(I version);

    Flux<Tuple2<K, V>> streamEntries(I version);

}
