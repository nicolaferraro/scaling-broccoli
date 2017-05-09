package io.broccoli.sql.ast;

import lombok.Data;

/**
 * @author nicola
 * @since 09/05/2017
 */
@Data
public class ColumnDefinitionAST {

    private String name;

    private ColumnTypeAST type;

}
