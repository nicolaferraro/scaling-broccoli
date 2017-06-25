package io.broccoli.sql.parser;

import io.broccoli.sql.BroccoliBaseListener;
import io.broccoli.sql.BroccoliParser;
import io.broccoli.sql.ast.DatabaseAST;
import io.broccoli.sql.ast.TableDefinitionAST;
import io.broccoli.sql.ast.ViewDefinitionAST;

import javaslang.collection.List;

import static io.broccoli.sql.parser.ParserASTConversions.toAST;

/**
 * @author nicola
 * @since 09/05/2017
 */
class BroccoliDatabaseListener extends BroccoliBaseListener {

    private DatabaseAST database;

    @Override
    public void enterSqlFile(BroccoliParser.SqlFileContext ctx) {
        database = new DatabaseAST();
        database.setTables(List.empty());
        database.setViews(List.empty());
    }

    /*
     * Tables
     */

    @Override
    public void enterCreateTableStatement(BroccoliParser.CreateTableStatementContext ctx) {
        TableDefinitionAST table = toAST(ctx);
        database.setTables(database.getTables().append(table));
    }

    /*
     * Views
     */

    @Override
    public void enterCreateViewStatement(BroccoliParser.CreateViewStatementContext ctx) {
        ViewDefinitionAST view = toAST(ctx);
        database.setViews(database.getViews().append(view));
    }

    /*
     * Build
     */

    public DatabaseAST build() {
        return database;
    }

}
