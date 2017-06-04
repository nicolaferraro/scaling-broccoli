package io.broccoli.sql.ast;

import javaslang.control.Option;
import lombok.Data;

/**
 * @author nicola
 * @since 21/05/2017
 */
@Data
public class SourceSelectionAST {

    private String name;

    private String alias;

    public String getNameOrAlias() {
        return Option.of(alias).getOrElse(name);
    }

}
