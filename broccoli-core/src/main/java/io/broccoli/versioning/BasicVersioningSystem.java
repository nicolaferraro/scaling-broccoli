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
public class BasicVersioningSystem implements VersioningSystem {

    private static final char FILLER = '0';
    private static final int PAD_LENGTH = 10;

    private AtomicLong counter = new AtomicLong(0);
    private ConcurrentHashMap<String, AtomicLong> subCounters = new ConcurrentHashMap<>();

    public static void main(String... args) {
        System.out.println("aaaaa".compareTo("aaaa"));
    }

    @Override
    public Version zero() {
        return new StringVersion(pad(0, FILLER));
    }

    @Override
    public Version next() {
        return new StringVersion(pad(counter.incrementAndGet(), FILLER) + "." + pad(9, '9'));
    }

    @Override
    public Version get(long counter) {
        return new StringVersion(pad(counter, FILLER) + "." + pad(9, '9'));
    }

    @Override
    public Version newSubVersion(Version version) {
        String raw = ((StringVersion) rawVersion(version)).v;
        subCounters.putIfAbsent(raw, new AtomicLong(0));
        return new StringVersion(raw + "." + pad(subCounters.get(raw).incrementAndGet(), FILLER));
    }

    @Override
    public Version rawVersion(Version subVersion) {
        return new StringVersion(((StringVersion) subVersion).v.substring(0, PAD_LENGTH));
    }

    @Override
    public boolean isRawVersion(Version version) {
        return ((StringVersion) version).v.endsWith(pad(9, '9'));
    }

    private String pad(Object o, char filler) {
        if (o == null) {
            return null;
        }
        String str = o.toString();
        while (str.length() < PAD_LENGTH) {
            str = filler + str;
        }
        return str;
    }

    private static class StringVersion implements Version {
        private String v;

        private StringVersion(String v) {
            this.v = v;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            StringVersion that = (StringVersion) o;

            return v != null ? v.equals(that.v) : that.v == null;

        }

        @Override
        public int hashCode() {
            return v != null ? v.hashCode() : 0;
        }

        @Override
        public int compareTo(Version version) {
            if (!(version instanceof StringVersion)) {
                throw new IllegalArgumentException("Cannot compare versions");
            }
            StringVersion sv = (StringVersion) version;
            return this.v.compareTo(sv.v);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("StringVersion{");
            sb.append("v='").append(v).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

}
