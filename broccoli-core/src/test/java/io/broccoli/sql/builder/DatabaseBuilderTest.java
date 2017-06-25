package io.broccoli.sql.builder;

import java.io.IOException;
import java.time.Duration;

import io.broccoli.core.Database;
import io.broccoli.core.Query;
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
public class DatabaseBuilderTest {

    @Test
    public void testBuildSalesDatabase() throws IOException {

        DatabaseBuilder builder = new DatabaseBuilder();
        Database db = builder.build(getClass().getResourceAsStream("/sales.sql"));

        VersioningSystem v = db.versioningSystem();

        Flux.just(
                TestEventFactory.add("products", v.next(), 1, "iPhone 7", "Apple iPhone 7"),
                TestEventFactory.add("products", v.next(), 2, "OnePlus 3", "OnePlus 3"),
                TestEventFactory.add("sales", v.next(), 1, 1, 10)
        ).subscribe(db.subscriber());

        db.start();
        db.currentVersion().last().block(Duration.ofSeconds(5)); // wait for db full

        Query result = db.newQueryBuilder()
                .query("select description from products")
                .build();

        result.changes().last().block(Duration.ofSeconds(5));


        List<Row> rows = List.ofAll(result.stream(v.current()).collectList().block());
        assertEquals(2, rows.size());

        assertEquals("Apple iPhone 7", rows.get(0).cell(0));
        assertEquals("OnePlus 3", rows.get(1).cell(0));

        Query result2 = db.newQueryBuilder()
                .query("select s.price from sales_full")
                .build();

        result2.changes().collectList().block(Duration.ofSeconds(5));
        assertEquals(1, result2.stream(v.current()).collectList().block().size());
    }

}
