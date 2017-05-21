package io.broccoli.builder;

import java.io.IOException;
import java.io.InputStream;

import io.broccoli.core.Database;
import io.broccoli.core.Table;
import io.broccoli.core.basic.BasicDatabase;
import io.broccoli.sql.ast.DatabaseAST;
import io.broccoli.sql.ast.TableDefinitionAST;
import io.broccoli.sql.parser.BroccoliDatabaseParser;
import io.broccoli.versioning.BasicVersioningSystem;
import io.broccoli.versioning.VersioningSystem;

/**
 * @author nicola
 * @since 21/05/2017
 */
public class DatabaseSqlBuilder {

    private BroccoliDatabaseParser parser = new BroccoliDatabaseParser();

    public DatabaseSqlBuilder() {
    }

    public Database build(InputStream is) throws IOException {
        DatabaseAST ast = parser.build(is);

        VersioningSystem v = new BasicVersioningSystem();
        Database.Builder db = new BasicDatabase.Builder(v);

        for (TableDefinitionAST table : ast.getTables()) {
            // TODO
        }

        return null;
    }

    public Database build(String content) {
        DatabaseAST database = parser.build(content);
        return null;
    }

    private Table fromAST(TableDefinitionAST ast) {
        return null;
    }

}
