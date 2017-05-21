package io.broccoli.sql.ast;

import javaslang.collection.List;
import lombok.Data;

/**
 * @author nicola
 * @since 09/05/2017
 */
@Data
public class ViewDefinitionAST {

    private String name;

    private SelectStatementAST query;

}
