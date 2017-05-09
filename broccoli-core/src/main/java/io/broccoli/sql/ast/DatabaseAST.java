package io.broccoli.sql.ast;

import javaslang.collection.List;
import lombok.Data;

/**
 * @author nicola
 * @since 09/05/2017
 */
@Data
public class DatabaseAST {

    private List<TableAST> tables;

}
