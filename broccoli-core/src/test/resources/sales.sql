-- noinspection SqlNoDataSourceInspectionForFile

-- noinspection SqlDialectInspectionForFile

/*
 * Sales database.
 */

CREATE TABLE products (
  id            INTEGER,
  name          VARCHAR,
  description   VARCHAR
);

CREATE TABLE sales (
  id            INTEGER,
  product_id    INTEGER,
  price         INTEGER
);

CREATE VIEW sales_full AS (
  SELECT p.*, s.*
  FROM products p, sales s
  WHERE p.id = s.product_id
);
