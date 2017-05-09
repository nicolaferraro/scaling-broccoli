package io.broccoli.sql.ast;

import javaslang.collection.List;
import lombok.Data;
import lombok.Value;

/**
 * @author nicola
 * @since 09/05/2017
 */
@Data
public class TableAST {

    private String name;

    private List<ColumnDefinitionAST> columns;

}
