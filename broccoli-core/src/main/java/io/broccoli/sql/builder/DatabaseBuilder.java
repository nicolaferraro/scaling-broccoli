package io.broccoli.sql.builder;

import java.io.IOException;
import java.io.InputStream;

import io.broccoli.core.Database;
import io.broccoli.sql.ast.DatabaseAST;
import io.broccoli.sql.parser.BroccoliDatabaseParser;

/**
 * @author nicola
 * @since 21/05/2017
 */
public class DatabaseBuilder {

    private BroccoliDatabaseParser parser = new BroccoliDatabaseParser();

    public DatabaseBuilder() {
    }

    public Database build(InputStream is) throws IOException {
        DatabaseAST ast = parser.build(is);
        return BuilderASTConversions.fromAST(ast);
    }

    public Database build(String content) {
        DatabaseAST ast = parser.build(content);
        return BuilderASTConversions.fromAST(ast);
    }

}
