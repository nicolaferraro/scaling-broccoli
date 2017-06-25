package io.broccoli.sql.parser;

import io.broccoli.sql.BroccoliParser;
import io.broccoli.sql.ast.ColumnDefinitionAST;
import io.broccoli.sql.ast.ColumnTypeAST;
import io.broccoli.sql.ast.ExpressionAST;
import io.broccoli.sql.ast.ResultColumnAST;
import io.broccoli.sql.ast.SelectStatementAST;
import io.broccoli.sql.ast.SourceSelectionAST;
import io.broccoli.sql.ast.TableDefinitionAST;
import io.broccoli.sql.ast.ViewDefinitionAST;

import javaslang.collection.List;

/**
 * @author nicola
 * @since 25/06/2017
 */
public final class ParserASTConversions {

    private ParserASTConversions() {
    }

     /*
     * Tables
     */

    public static TableDefinitionAST toAST(BroccoliParser.CreateTableStatementContext ctx) {
        TableDefinitionAST table = new TableDefinitionAST();
        table.setName(ctx.tableName().getText());
        table.setColumns(List.ofAll(ctx.columnDefinitionList().columnDefinition()).map(ParserASTConversions::toAST));
        return table;
    }

    public static ColumnDefinitionAST toAST(BroccoliParser.ColumnDefinitionContext ctx) {
        ColumnDefinitionAST column = new ColumnDefinitionAST();
        column.setName(ctx.columnName().getText());
        column.setType(ColumnTypeAST.valueOf(ctx.type().getText().toUpperCase()));
        return column;
    }

    /*
    * Views
    */

    public static ViewDefinitionAST toAST(BroccoliParser.CreateViewStatementContext ctx) {
        ViewDefinitionAST view = new ViewDefinitionAST();
        view.setName(ctx.viewName().getText());
        view.setQuery(toAST(ctx.selectStatement()));
        return view;
    }

    /*
     * Query
     */

    public static SelectStatementAST toAST(BroccoliParser.SelectStatementContext ctx) {
        SelectStatementAST select = new SelectStatementAST();
        select.setResultColumns(List.ofAll(ctx.resultColumn()).map(ParserASTConversions::toAST));
        select.setSourceSelections(List.ofAll(ctx.tableWithOptionalAlias()).map(ParserASTConversions::toAST));
        if (ctx.expr() != null) {
            select.setFilter(toAST(ctx.expr()));
        }
        return select;
    }

    public static ResultColumnAST toAST(BroccoliParser.ResultColumnContext ctx) {
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

    public static SourceSelectionAST toAST(BroccoliParser.TableWithOptionalAliasContext ctx) {
        SourceSelectionAST source = new SourceSelectionAST();
        source.setName(ctx.tableName().getText());
        if (ctx.tableAlias() != null) {
            source.setAlias(ctx.tableAlias().getText());
        } else {
            source.setAlias(ctx.tableName().getText());
        }
        return source;
    }

    public static ExpressionAST toAST(BroccoliParser.ExprContext ctx) {
        return ctx.accept(new ExpressionVisitor());
    }

}
