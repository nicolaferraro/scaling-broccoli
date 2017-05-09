package io.broccoli.sql;

import io.broccoli.sql.ast.ColumnDefinitionAST;
import io.broccoli.sql.ast.ColumnTypeAST;
import io.broccoli.sql.ast.DatabaseAST;
import io.broccoli.sql.ast.TableAST;

import javaslang.collection.List;

/**
 * @author nicola
 * @since 09/05/2017
 */
public class BroccoliDatabaseListener extends BroccoliBaseListener {

    private DatabaseAST currentDatabase;

    private TableAST currentTable;

    @Override
    public void enterSqlFile(BroccoliParser.SqlFileContext ctx) {
        currentDatabase = new DatabaseAST();
        currentDatabase.setTables(List.empty());
    }

    @Override
    public void enterTableName(BroccoliParser.TableNameContext ctx) {
        currentTable = new TableAST();
        currentTable.setName(ctx.getText());
        currentTable.setColumns(List.empty());
        currentDatabase.setTables(currentDatabase.getTables().append(currentTable));
    }

    @Override
    public void enterColumnDefinition(BroccoliParser.ColumnDefinitionContext ctx) {
        ColumnDefinitionAST column = new ColumnDefinitionAST();
        column.setName(ctx.columnName().getText());
        column.setType(ColumnTypeAST.valueOf(ctx.type().getText().toUpperCase()));
        currentTable.setColumns(currentTable.getColumns().append(column));
    }

    public DatabaseAST build() {
        return currentDatabase;
    }

}
