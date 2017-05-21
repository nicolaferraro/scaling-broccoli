package io.broccoli.sql.ast;

import java.math.BigDecimal;

import lombok.Data;

/**
 * @author nicola
 * @since 21/05/2017
 */
@Data
public class ExpressionLiteralAST extends ExpressionAST {

    private BigDecimal numericValue;

    private String stringValue;

}
