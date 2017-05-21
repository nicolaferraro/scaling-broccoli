package io.broccoli.sql.ast;

import javaslang.collection.List;
import javaslang.collection.Map;
import lombok.Data;

/**
 * @author nicola
 * @since 10/05/2017
 */
@Data
public class SelectStatementAST {

    private List<ResultColumnAST> resultColumns;

    private List<SourceSelectionAST> sourceSelections;

    private ExpressionAST filter;

}
