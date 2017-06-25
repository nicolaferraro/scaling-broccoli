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
package io.broccoli.sql.parser;

import io.broccoli.sql.BroccoliLexer;
import io.broccoli.sql.BroccoliParser;
import io.broccoli.sql.ast.SelectStatementAST;

import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

/**
 * @author nicola
 * @since 25/06/2017
 */
public class BroccoliQueryParser {

    public BroccoliQueryParser() {
    }

    public SelectStatementAST build(String content) {
        return build(CharStreams.fromString(content));
    }

    public SelectStatementAST build(CharStream stream) {
        BroccoliLexer lexer = new BroccoliLexer(stream);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        BroccoliParser parser = new BroccoliParser(tokenStream);
        parser.setErrorHandler(new BailErrorStrategy());
        BroccoliParser.SelectStatementContext tree = parser.selectStatement();

        ParseTreeWalker walker = new ParseTreeWalker();
        BroccoliQueryListener listener = new BroccoliQueryListener();
        walker.walk(listener, tree);

        SelectStatementAST select = listener.build();
        return select;
    }

}
