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
package io.broccoli.sql;

import io.broccoli.stream.Database;
import io.broccoli.stream.Query;
import io.broccoli.stream.Table;
import io.broccoli.versioning.Version;

import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

/**
 * @author nicola
 * @since 19/04/2017
 */
public class BroccoliQueryParser {

    private Database database;

    private Version version;

    public BroccoliQueryParser(Database database, Version version) {
        this.database = database;
        this.version = version;
    }

    public Table parseQuery(String query) {
        CharStream stream = CharStreams.fromString(query);
        BroccoliLexer lexer = new BroccoliLexer(stream);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        BroccoliParser parser = new BroccoliParser(tokenStream);
        BroccoliSqlListener listener = new BroccoliSqlListener(database, version);
        parser.addParseListener(listener);
        parser.setErrorHandler(new BailErrorStrategy());
        parser.parse();

        return listener.build();
    }

}
