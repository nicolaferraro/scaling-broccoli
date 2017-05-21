package io.broccoli.sql;


import io.broccoli.sql.ast.ColumnTypeAST;
import io.broccoli.sql.ast.DatabaseAST;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author nicola
 * @since 09/05/2017
 */
public class DatabaseParserTest {

    @Test
    public void testSingleTable() {
        BroccoliDatabaseBuilder builder = new BroccoliDatabaseBuilder();
        DatabaseAST db = builder.build("create table xxx (id \nVARCHAR)");

        assertEquals(1, db.getTables().size());
        assertEquals("xxx", db.getTables().head().getName());
        assertEquals(1, db.getTables().head().getColumns().size());
        assertEquals("id", db.getTables().head().getColumns().head().getName());
        assertEquals(ColumnTypeAST.VARCHAR, db.getTables().head().getColumns().head().getType());
    }

    @Test
    public void testMultipleTable() {
        BroccoliDatabaseBuilder builder = new BroccoliDatabaseBuilder();
        DatabaseAST db = builder.build("create table xxx (id VARCHAR); create table yyy (name INTEGER, value VARCHAR)");

        assertEquals(2, db.getTables().size());
        assertEquals("xxx", db.getTables().get(0).getName());
        assertEquals(1, db.getTables().get(0).getColumns().size());
        assertEquals("id", db.getTables().get(0).getColumns().get(0).getName());
        assertEquals(ColumnTypeAST.VARCHAR, db.getTables().get(0).getColumns().get(0).getType());

        assertEquals("yyy", db.getTables().get(1).getName());
        assertEquals(2, db.getTables().get(1).getColumns().size());
        assertEquals("name", db.getTables().get(1).getColumns().get(0).getName());
        assertEquals(ColumnTypeAST.INTEGER, db.getTables().get(1).getColumns().get(0).getType());
        assertEquals("value", db.getTables().get(1).getColumns().get(1).getName());
        assertEquals(ColumnTypeAST.VARCHAR, db.getTables().get(1).getColumns().get(1).getType());
    }

    @Test(expected = RuntimeException.class)
    public void testDuplicateTableName() {
        BroccoliDatabaseBuilder builder = new BroccoliDatabaseBuilder();
        builder.build("create table xxx (id VARCHAR); create table xxx (name INTEGER, value VARCHAR)");
    }

    @Test(expected = RuntimeException.class)
    public void testDuplicateColumnName() {
        BroccoliDatabaseBuilder builder = new BroccoliDatabaseBuilder();
        builder.build("create table xxx (id VARCHAR); create table yyy (name INTEGER, name VARCHAR)");
    }

    @Test
    public void testParseQuery() {
        BroccoliDatabaseBuilder builder = new BroccoliDatabaseBuilder();
        builder.build("create table xxx (id VARCHAR); create materialized view yyy as (select 1+2*(3%1), xxx.* from xxx where 1=1 or 2<4)");
    }

    @Test
    public void testParseDB() throws Exception {
        BroccoliDatabaseBuilder builder = new BroccoliDatabaseBuilder();
        DatabaseAST db = builder.build(getClass().getResourceAsStream("/sales.sql"));
        assertTrue(db.getTables().size() >= 2);
        assertTrue(db.getViews().size() >= 1);
    }

}
