package io.broccoli.builder;

import java.io.IOException;
import java.io.InputStream;

import io.broccoli.core.Database;
import io.broccoli.core.Event;
import io.broccoli.core.Table;
import io.broccoli.core.TableEvent;
import io.broccoli.core.Type;
import io.broccoli.core.basic.BasicDatabase;
import io.broccoli.core.basic.BasicFluxStreamable;
import io.broccoli.core.basic.BasicSetCacheStreamable;
import io.broccoli.sql.ast.ColumnDefinitionAST;
import io.broccoli.sql.ast.ColumnTypeAST;
import io.broccoli.sql.ast.DatabaseAST;
import io.broccoli.sql.ast.TableDefinitionAST;
import io.broccoli.sql.parser.BroccoliDatabaseParser;
import io.broccoli.versioning.BasicVersioningSystem;
import io.broccoli.versioning.VersioningSystem;

import javaslang.collection.List;
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

        Database.Builder db = new BasicDatabase.Builder(v)
                .eventsProcessor(events);

        for (TableDefinitionAST tableAST : ast.getTables()) {
            Table table = fromAST(tableAST, events, v);
            db = db.sourceTable(table);
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
                .filter(e -> e.table().equals(ast.getName()))
                .map(Event.class::cast);

        BasicFluxStreamable streamable = new BasicFluxStreamable(ast.getName(), names, types, flux);
        return new BasicSetCacheStreamable(ast.getName(), streamable, v);
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

}
