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

/**
 * @author nicola
 * @since 13/04/2017
 */
public final class Arguments {

    private Arguments() {
    }

    public static <T> T requireNonNull(T o, String name) {
        if (o == null) {
            throw new IllegalArgumentException("Object " + name + " cannot be null");
        }
        return o;
    }

    public static int requireRange(int num, int min, int max) {
        if (num < min || num > max) {
            throw new IllegalArgumentException("Number out of range: " + num + " is not included in [" + min + ", " + max + "]");
        }
        return num;
    }

}
