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
package io.broccoli.versioning;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author nicola
 * @since 14/04/2017
 */
public class BasicVersioningSystem implements VersioningSystem<String> {

    private static final char FILLER = '0';
    private static final int PAD_LENGTH = 10;

    private AtomicLong counter = new AtomicLong(0);
    private ConcurrentHashMap<String, AtomicLong> subCounters = new ConcurrentHashMap<>();

    public static void main(String... args) {
        System.out.println("aaaaa".compareTo("aaaa"));
    }

    @Override
    public String zero() {
        return pad(FILLER);
    }

    @Override
    public String next() {
        return pad(counter.incrementAndGet());
    }

    @Override
    public String get(long counter) {
        return pad(counter);
    }

    @Override
    public String newSubVersion(String version) {
        subCounters.putIfAbsent(version, new AtomicLong(0));
        return version + "." + pad(subCounters.get(version).incrementAndGet());
    }

    @Override
    public String rawVersion(String subVersion) {
        return subVersion.substring(PAD_LENGTH);
    }

    private String pad(Object o) {
        if (o == null) {
            return null;
        }
        String str = o.toString();
        while (str.length() < PAD_LENGTH) {
            str = FILLER + str;
        }
        return str;
    }
}
