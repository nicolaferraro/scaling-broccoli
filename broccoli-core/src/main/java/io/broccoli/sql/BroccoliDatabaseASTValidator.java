package io.broccoli.sql;

import io.broccoli.sql.ast.DatabaseAST;
import io.broccoli.sql.ast.TableAST;

import javaslang.collection.List;

/**
 * @author nicola
 * @since 10/05/2017
 */
public class BroccoliDatabaseASTValidator {

    private List<Constraint> constraints;

    public BroccoliDatabaseASTValidator() {

        constraints = List.of(
                d -> {
                    // No duplicate tables
                    if (d.getTables().map(t -> t.getName()).distinct().size() < d.getTables().size()) {
                        return List.of("Duplicate table names");
                    }
                    return List.empty();
                },
                d -> {
                    // No duplicate columns
                    for (TableAST table : d.getTables()) {
                        if (table.getColumns().map(c -> c.getName()).distinct().size() < table.getColumns().size()) {
                            return List.of("Duplicate column names in table " + table.getName());
                        }
                    }

                    return List.empty();
                }
        );

    }

    public List<String> validate(DatabaseAST database) {
        return constraints.flatMap(c -> c.validate(database));
    }

    @FunctionalInterface
    public interface Constraint {

        List<String> validate(DatabaseAST database);

    }

}
