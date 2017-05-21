package io.broccoli.sql.ast;

import lombok.Data;

/**
 * @author nicola
 * @since 21/05/2017
 */
@Data
public class ExpressionBinaryOperationAST extends ExpressionAST {

    private BinaryOperationAST operation;

    private ExpressionAST leftExpression;

    private ExpressionAST rightExpression;

    public enum BinaryOperationAST {
        MULT,
        DIV,
        MOD,
        PLUS,
        MINUS,
        LT,
        LTE,
        GT,
        GTE,
        EQ,
        NOT_EQ,
        LIKE,
        AND,
        OR
    }

}
