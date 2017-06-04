package io.broccoli.builder;

import java.io.IOException;
import java.time.Duration;

import io.broccoli.core.Database;
import io.broccoli.core.Row;
import io.broccoli.core.Table;
import io.broccoli.util.TestEventFactory;
import io.broccoli.versioning.VersioningSystem;

import org.junit.Test;

import javaslang.collection.List;
import reactor.core.publisher.Flux;

import static org.junit.Assert.assertEquals;

/**
 * @author nicola
 * @since 02/06/2017
 */
public class DatabaseSqlBuilderTest {

    @Test
    public void testBuildSalesDatabase() throws IOException {

        DatabaseSqlBuilder builder = new DatabaseSqlBuilder();
        Database db = builder.build(getClass().getResourceAsStream("/sales.sql"));

        VersioningSystem v = db.versioningSystem();

        Flux.just(
                TestEventFactory.add("products", v.next(), 1, "iPhone 7", "Apple iPhone 7"),
                TestEventFactory.add("products", v.next(), 2, "OnePlus 3", "OnePlus 3"),
                TestEventFactory.add("sales", v.next(), 1, 1, 10)
        ).subscribe(db.subscriber());

        db.start();
        db.currentVersion().last().block(Duration.ofSeconds(5)); // wait for db full

        Table result = db.newQueryBuilder()
                .select("description")
                .from("products")
                .buildQuery(v.current());

        result.changes().last().block(Duration.ofSeconds(5));


        List<Row> rows = List.ofAll(result.stream(v.current()).collectList().block());
        assertEquals(2, rows.size());

        assertEquals("Apple iPhone 7", rows.get(0).cell(0));
        assertEquals("OnePlus 3", rows.get(1).cell(0));

        Table result2 = db.newQueryBuilder()
                .select("s.price")
                .from("sales_full")
                .buildQuery(v.current());

        result2.changes().collectList().block(Duration.ofSeconds(5));
        assertEquals(1, result2.stream(v.current()).collectList().block().size());
    }

}
