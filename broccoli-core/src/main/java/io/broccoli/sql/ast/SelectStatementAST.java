package io.broccoli.sql.ast;

import javaslang.collection.List;
import lombok.Data;

/**
 * @author nicola
 * @since 10/05/2017
 */
@Data
public class SelectStatementAST {

    List<ResultColumnAST> resultColumns;

}
