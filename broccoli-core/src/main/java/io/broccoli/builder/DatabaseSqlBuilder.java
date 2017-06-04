package io.broccoli.builder;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.broccoli.core.Database;
import io.broccoli.core.Event;
import io.broccoli.core.Expression;
import io.broccoli.core.Row;
import io.broccoli.core.Streamable;
import io.broccoli.core.Structured;
import io.broccoli.core.Table;
import io.broccoli.core.TableEvent;
import io.broccoli.core.Type;
import io.broccoli.core.basic.BasicCartesianStreamable;
import io.broccoli.core.basic.BasicDatabase;
import io.broccoli.core.basic.BasicFilterStreamable;
import io.broccoli.core.basic.BasicFluxStreamable;
import io.broccoli.core.basic.BasicNoopEvent;
import io.broccoli.core.basic.BasicSetCacheStreamable;
import io.broccoli.core.basic.BasicTableRenamer;
import io.broccoli.sql.ast.ColumnDefinitionAST;
import io.broccoli.sql.ast.ColumnTypeAST;
import io.broccoli.sql.ast.DatabaseAST;
import io.broccoli.sql.ast.ExpressionAST;
import io.broccoli.sql.ast.ExpressionBinaryOperationAST;
import io.broccoli.sql.ast.ExpressionColumnAST;
import io.broccoli.sql.ast.ExpressionLiteralAST;
import io.broccoli.sql.ast.ExpressionUnaryOperatorAST;
import io.broccoli.sql.ast.ResultColumnAST;
import io.broccoli.sql.ast.SelectStatementAST;
import io.broccoli.sql.ast.TableDefinitionAST;
import io.broccoli.sql.ast.ViewDefinitionAST;
import io.broccoli.sql.parser.BroccoliDatabaseParser;
import io.broccoli.versioning.BasicVersioningSystem;
import io.broccoli.versioning.VersioningSystem;

import javaslang.collection.HashMap;
import javaslang.collection.List;
import javaslang.collection.Map;
import reactor.core.publisher.Flux;
import reactor.core.publisher.TopicProcessor;

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
        TopicProcessor<TableEvent> events = TopicProcessor.create();
        Map<String, Table> tables = HashMap.empty();

        Database.Builder db = new BasicDatabase.Builder(v)
                .eventsProcessor(events);

        for (TableDefinitionAST tableAST : ast.getTables()) {
            Table table = fromAST(tableAST, events, v);
            tables = tables.put(table.name(), table);
            db = db.sourceTable(table);
        }

        for (ViewDefinitionAST viewAST : ast.getViews()) {
            Table view = fromAST(viewAST, events, v, tables);
            tables = tables.put(view.name(), view);
            db = db.sourceTable(view);
        }

        return db.build();
    }

    public Database build(String content) {
        DatabaseAST database = parser.build(content);
        return null;
    }

    private Table fromAST(TableDefinitionAST ast, TopicProcessor<TableEvent> events, VersioningSystem v) {
        List<String> names = ast.getColumns().map(ColumnDefinitionAST::getName);
        List<Type> types = ast.getColumns().map(def -> fromAST(def.getType()));

        Flux<Event> flux = events
                .map(e -> noopIfNoMatch(e, ast.getName()))
                .map(Event.class::cast);

        BasicFluxStreamable streamable = new BasicFluxStreamable(ast.getName(), names, types, flux);
        return new BasicSetCacheStreamable(ast.getName(), streamable, v);
    }

    private Table fromAST(ViewDefinitionAST ast, TopicProcessor<TableEvent> events, VersioningSystem v, Map<String, Table> tables) {
        Streamable query = fromAST(ast.getQuery(), events, v, tables);
        return new BasicSetCacheStreamable(ast.getName(), query, v);
    }

    private Streamable fromAST(SelectStatementAST ast, TopicProcessor<TableEvent> events, VersioningSystem v, Map<String, Table> tables) {
        List<BasicTableRenamer> sources = ast
                .getSourceSelections()
                .map(ss -> new BasicTableRenamer(
                        tables.get(ss.getName()).getOrElseThrow(() -> new IllegalArgumentException("Unknown table " + ss.getName())), ss.getNameOrAlias())
                );


        BasicCartesianStreamable cartesian = new BasicCartesianStreamable(
                sources.map(BasicTableRenamer::name).mkString("cartesian(", ",", ")"),
                v,
                sources.toJavaArray(BasicTableRenamer.class)
        );


        Expression fun = fromAST(ast.getFilter(), cartesian);
        Predicate<Row> filter = row -> {
            Object res = fun.evaluate(row);
            if (res instanceof Boolean) {
                return (Boolean) res;
            } else {
                throw new IllegalArgumentException("Wrong predicate return type " + res.getClass());
            }
        };

        BasicFilterStreamable filtered = new BasicFilterStreamable("filter", filter, cartesian);

        return filtered;
    }

    private Expression fromAST(ExpressionAST expressionAST, Structured schema) {
        if (expressionAST instanceof ExpressionUnaryOperatorAST) {
            return fromAST((ExpressionUnaryOperatorAST)expressionAST, schema);
        } else if (expressionAST instanceof ExpressionBinaryOperationAST) {
            return fromAST((ExpressionBinaryOperationAST)expressionAST, schema);
        } else if (expressionAST instanceof ExpressionLiteralAST) {
            return fromAST((ExpressionLiteralAST)expressionAST, schema);
        } else if (expressionAST instanceof ExpressionColumnAST) {
            return fromAST((ExpressionColumnAST)expressionAST, schema);
        } else {
            throw new IllegalStateException("Unsupported expression type " + expressionAST.getClass());
        }
    }

    private Expression fromAST(ExpressionUnaryOperatorAST expressionAST, Structured schema) {
        Supplier<Expression> sub = () -> fromAST(expressionAST.getSubExpression(), schema);

        switch (expressionAST.getOperator()) {
        case IS_NOT_NULL:
            return row -> sub.get().evaluate(row) != null;
        default:
            throw new IllegalStateException("Unsupported operator " + expressionAST.getOperator());
        }
    }

    private Expression fromAST(ExpressionBinaryOperationAST expressionAST, Structured schema) {
        Supplier<Expression> left = () -> fromAST(expressionAST.getLeftExpression(), schema);
        Supplier<Expression> right = () -> fromAST(expressionAST.getRightExpression(), schema);

        switch (expressionAST.getOperation()) {
        case EQ:
            return row -> left.get().evaluate(row).equals(right.get().evaluate(row));
        default:
            throw new IllegalStateException("Unsupported operation " + expressionAST.getOperation());
        }
    }

    private Expression fromAST(ExpressionLiteralAST expressionAST, Structured schema) {
        return new Expression() {
            @Override
            public Object evaluate(Row row) {
                if (expressionAST.getStringValue() != null) {
                    return expressionAST.getStringValue();
                } else if (expressionAST.getNumericValue() != null) {
                    return expressionAST.getNumericValue();
                } else {
                    throw new IllegalStateException("Cannot evaluate literal expression");
                }
            }
        };
    }

    private Expression fromAST(ExpressionColumnAST expressionAST, Structured schema) {
        return new Expression() {
            int pos;
            {
                if (expressionAST.getTableName() == null) {
                    pos = schema.unqualifiedNames().indexOf(expressionAST.getColumnName());
                } else {
                    pos = schema.names().indexOf(expressionAST.getTableName() + "." + expressionAST.getColumnName());
                }
            }

            @Override
            public Object evaluate(Row row) {
                return row.cell(pos);
            }
        };
    }

    private Type fromAST(ColumnTypeAST ast) {
        switch (ast) {
        case INTEGER:
            return Type.INTEGER;
        case VARCHAR:
            return Type.STRING;
        default:
            throw new IllegalStateException("Unsupported type: " + ast);
        }
    }

    private Event noopIfNoMatch(TableEvent event, String table) {
        if (table.equals(event.table())) {
            return event;
        } else {
            return new BasicNoopEvent(event.version());
        }
    }

}
