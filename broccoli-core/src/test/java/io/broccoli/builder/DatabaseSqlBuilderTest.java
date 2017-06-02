package io.broccoli.builder;

import java.io.IOException;
import java.time.Duration;

import io.broccoli.core.Database;
import io.broccoli.core.Table;
import io.broccoli.util.TestEventFactory;
import io.broccoli.versioning.VersioningSystem;

import org.junit.Ignore;
import org.junit.Test;

import reactor.core.publisher.Flux;

/**
 * @author nicola
 * @since 02/06/2017
 */
public class DatabaseSqlBuilderTest {

    @Test
    @Ignore("fix")
    public void testBuildSalesDatabase() throws IOException {

        DatabaseSqlBuilder builder = new DatabaseSqlBuilder();
        Database db = builder.build(getClass().getResourceAsStream("/sales.sql"));

        VersioningSystem v = db.versioningSystem();

        Flux.just(
                TestEventFactory.add("products", v.next(), 1, "iPhone 7", "Apple iPhone 7"),
                TestEventFactory.add("products", v.next(), 2, "OnePlus 3", "OnePlus 3")
        ).subscribe(db.subscriber());

        db.start();
        db.currentVersion().last().block(Duration.ofSeconds(5)); // wait for db full

        Table result = db.newQueryBuilder()
                .select("description")
                .from("product")
                .buildQuery(v.current());
        result.changes().last().block(Duration.ofSeconds(5));





    }

}
