package io.broccoli.sql.builder;

import io.broccoli.core.Database;
import io.broccoli.core.Event;
import io.broccoli.core.Query;
import io.broccoli.core.Row;
import io.broccoli.core.Streamable;
import io.broccoli.core.Table;
import io.broccoli.core.Type;
import io.broccoli.core.basic.BasicSetCacheStreamable;
import io.broccoli.sql.ast.SelectStatementAST;
import io.broccoli.sql.parser.BroccoliQueryParser;
import io.broccoli.versioning.Version;

import javaslang.collection.List;
import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 21/05/2017
 */
public class QueryBuilder {

    private Database database;

    private BroccoliQueryParser parser = new BroccoliQueryParser();

    public QueryBuilder(Database database) {
        this.database = database;
    }

    public Query build(String sql, Version version) {
        SelectStatementAST ast = parser.build(sql);
        Streamable stream = BuilderASTConversions.fromAST(ast, database.versioningSystem(), database.tables().groupBy(Table::name).mapValues(t -> t.toList().head()), true, version);
        Table source = new BasicSetCacheStreamable("result", stream, database.versioningSystem());
        return new Query() {
            @Override
            public Flux<Row> stream(Version version) {
                return source.stream(version);
            }

            @Override
            public String name() {
                return source.name();
            }

            @Override
            public Flux<Event> changes() {
                return source.changes();
            }

            @Override
            public boolean monotonic() {
                return source.monotonic();
            }

            @Override
            public List<String> names() {
                return source.names();
            }

            @Override
            public List<Type> types() {
                return source.types();
            }
        };
    }

}
