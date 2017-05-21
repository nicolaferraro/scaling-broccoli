package io.broccoli.sql;

import io.broccoli.sql.ast.ColumnDefinitionAST;
import io.broccoli.sql.ast.ColumnTypeAST;
import io.broccoli.sql.ast.DatabaseAST;
import io.broccoli.sql.ast.ExpressionAST;
import io.broccoli.sql.ast.ResultColumnAST;
import io.broccoli.sql.ast.SelectStatementAST;
import io.broccoli.sql.ast.TableAST;
import io.broccoli.sql.ast.ViewAST;

import javaslang.collection.List;

/**
 * @author nicola
 * @since 09/05/2017
 */
public class BroccoliDatabaseListener extends BroccoliBaseListener {

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
        TableAST table = toAST(ctx);
        database.setTables(database.getTables().append(table));
    }

    public TableAST toAST(BroccoliParser.CreateTableStatementContext ctx) {
        TableAST table = new TableAST();
        table.setName(ctx.tableName().getText());
        table.setColumns(List.ofAll(ctx.columnDefinitionList().columnDefinition()).map(this::toAST));
        return table;
    }

    public ColumnDefinitionAST toAST(BroccoliParser.ColumnDefinitionContext ctx) {
        ColumnDefinitionAST column = new ColumnDefinitionAST();
        column.setName(ctx.columnName().getText());
        column.setType(ColumnTypeAST.valueOf(ctx.type().getText().toUpperCase()));
        return column;
    }

    /*
     * Views
     */

    @Override
    public void enterCreateViewStatement(BroccoliParser.CreateViewStatementContext ctx) {
        ViewAST view = toAST(ctx);
        database.setViews(database.getViews().append(view));
    }

    public ViewAST toAST(BroccoliParser.CreateViewStatementContext ctx) {
        ViewAST view = new ViewAST();
        view.setName(ctx.viewName().getText());
        view.setQuery(toAST(ctx.selectStatement()));
        return view;
    }

    /*
     * Query
     */

    public SelectStatementAST toAST(BroccoliParser.SelectStatementContext ctx) {
        SelectStatementAST select = new SelectStatementAST();
        select.setResultColumns(List.ofAll(ctx.resultColumn()).map(this::toAST));
        return select;
    }

    public ResultColumnAST toAST(BroccoliParser.ResultColumnContext ctx) {
        ResultColumnAST result = new ResultColumnAST();
        if (ctx.K_AS() != null) {
            result.setWildcard(true);
        } else if (ctx.tableName() != null) {
            result.setWildcard(true);
            result.setTableName(ctx.tableName().getText());
        } else if (ctx.expr() != null) {
            result.setExpression(toAST(ctx.expr()));
            if (ctx.columnAlias() != null) {
                result.setExpressionAlias(ctx.columnAlias().getText());
            }
        }
        return result;
    }

    public ExpressionAST toAST(BroccoliParser.ExprContext ctx) {
        return ctx.accept(new ExpressionVisitor());
    }

    /*
     * Build
     */

    public DatabaseAST build() {
        return database;
    }

}
