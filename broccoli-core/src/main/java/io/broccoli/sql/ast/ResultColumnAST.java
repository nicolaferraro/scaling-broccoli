package io.broccoli.sql.ast;

import lombok.Data;

/**
 * @author nicola
 * @since 21/05/2017
 */
@Data
public class ResultColumnAST {

    private boolean wildcard;

    private String tableName;

    private ExpressionAST expression;

    private String expressionAlias;

}
