package io.broccoli.core;

/**
 * @author nicola
 * @since 04/06/2017
 */
public interface Expression {

    Object evaluate(Row row);

}
