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

import java.io.IOException;
import java.io.InputStream;

import io.broccoli.sql.ast.DatabaseAST;
import io.broccoli.stream.Database;

import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import javaslang.collection.List;

/**
 * @author nicola
 * @since 09/05/2017
 */
public class BroccoliDatabaseBuilder {

    public BroccoliDatabaseBuilder() {
    }

    public DatabaseAST build(InputStream is) throws IOException {
        return build(CharStreams.fromStream(is));
    }

    public DatabaseAST build(String content) {
        return build(CharStreams.fromString(content));
    }

    public DatabaseAST build(CharStream stream) {
        BroccoliLexer lexer = new BroccoliLexer(stream);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        BroccoliParser parser = new BroccoliParser(tokenStream);
        parser.setErrorHandler(new BailErrorStrategy());
        BroccoliParser.SqlFileContext tree = parser.sqlFile();

        ParseTreeWalker walker = new ParseTreeWalker();
        BroccoliDatabaseListener listener = new BroccoliDatabaseListener();
        walker.walk(listener, tree);

        DatabaseAST database = listener.build();

        BroccoliASTValidator validator = new BroccoliASTValidator();
        List<String> errors = validator.validate(database);
        if (errors.nonEmpty()) {
            throw new RuntimeException("Error while building the database.\n" + errors.mkString("\n"));
        }
        return database;
    }

}
