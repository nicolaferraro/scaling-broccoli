package io.broccoli.sql.ast;

import java.math.BigDecimal;

import lombok.Data;

/**
 * @author nicola
 * @since 21/05/2017
 */
@Data
public class ExpressionColumnAST extends ExpressionAST {

    private String tableName;

    private String columnName;

}
