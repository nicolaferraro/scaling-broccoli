package io.broccoli.sql.ast;

import java.math.BigDecimal;

import lombok.Data;

/**
 * @author nicola
 * @since 21/05/2017
 */
@Data
public class ExpressionUnaryOperatorAST extends ExpressionAST {

    private UnaryOperatorAST operator;

    private ExpressionAST subExpression;

    public enum UnaryOperatorAST {
        PLUS,
        MINUS,
        NOT,
        IS_NULL,
        IS_NOT_NULL
    }

}
