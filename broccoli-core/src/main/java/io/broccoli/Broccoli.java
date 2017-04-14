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
package io.broccoli;

import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 12/04/2017
 */
public class Broccoli {

    public static void main(String[] args) {

        Flux.just("Hello", "world", "broccoli")
                .doOnNext(System.out::println)
                .subscribe();

    }

}
