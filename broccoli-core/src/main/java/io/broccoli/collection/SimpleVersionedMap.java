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

import io.broccoli.util.Arguments;

import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.collection.HashMap;
import javaslang.collection.List;
import javaslang.collection.Map;
import javaslang.control.Option;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 12/04/2017
 */
public class SimpleVersionedMap<K, V, I extends Comparable<? super I>> implements VersionedMap<K, V, I> {

    private volatile I maxVersion;

    private volatile Map<K, Values<V, I>> map;

    public SimpleVersionedMap(I zero) {
        this.maxVersion = Arguments.requireNonNull(zero, "zero");
        this.map = HashMap.empty();
    }

    @Override
    public void put(K key, V value, I version) {
        Arguments.requireNonNull(key, "key");
        checkEditable(version);
        Values<V, I> values = getOrCreate(key);
        values.setValue(value, version);
        this.maxVersion = version;
    }

    @Override
    public void delete(K key, I version) {
        checkEditable(version);
        Values<V, I> values = getOrCreate(key);
        values.delete(version);
        this.maxVersion = version;
    }

    @Override
    public Option<V> get(K key, I version) {
        return map.get(key).flatMap(vs -> vs.getValue(version));
    }

    @Override
    public I version() {
        return maxVersion;
    }

    @Override
    public Flux<V> streamValues(I version) {
        return Flux.fromIterable(map.values())
                .map(v -> v.getValue(version))
                .filter(Option::isDefined)
                .map(Option::get);
    }

    @Override
    public Flux<K> streamKeys(I version) {
        return Flux.fromIterable(map)
                .filter(t -> t._2.getValue(version).isDefined())
                .map(Tuple2::_1);
    }

    @Override
    public Flux<Tuple2<K, V>> streamEntries(I version) {
        return Flux.fromIterable(map)
                .map(t -> Tuple.of(t._1, t._2.getValue(version)))
                .filter(t -> t._2.isDefined())
                .map(t -> Tuple.of(t._1, t._2.get()));
    }

    private Values<V, I> getOrCreate(K key) {
        Option<Values<V, I>> values = map.get(key);
        if (values.isEmpty()) {
            Values<V, I> v = new Values<>();
            map = map.put(key, v);
            return v;
        }
        return values.get();
    }

    private void checkEditable(I version) {
        if (version.compareTo(maxVersion) <= 0) {
            throw new IllegalArgumentException("Can write on the last version only");
        }
    }


    static class Values<V, I extends Comparable<? super I>> {
        private volatile List<VersionedValue<V, I>> data = List.empty();
        private volatile List<I> deletions = List.empty();

        Values() {
        }

        Option<V> getValue(I version) {
            Option<VersionedValue<V, I>> maxData = maxData(version);
            Option<I> maxDeletion = maxDeletion(version);

            if (maxData.isEmpty()) {
                return Option.none();
            }

            if (maxDeletion.isEmpty() || maxDeletion.get().compareTo(maxData.get().created) < 0) {
                return Option.of(maxData.get().value);
            }

            return Option.none();
        }

        void setValue(V value, I version) {
            data = data.push(new VersionedValue<>(value, version));
        }

        void delete(I version) {
            deletions = deletions.push(version);
        }

        private Option<I> maxDeletion(I upTo) {
            return deletions.filter(d -> d.compareTo(upTo) <= 0).max();
        }

        private Option<VersionedValue<V, I>> maxData(I upTo) {
            return data.filter(d -> d.created.compareTo(upTo) <= 0).maxBy(vv -> vv.created);
        }

    }

    static class VersionedValue<V, I> {
        private V value;
        private I created;

        VersionedValue(V value, I created) {
            this.value = value;
            this.created = created;
        }

    }


}
