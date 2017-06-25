package io.broccoli.sql.parser;

import io.broccoli.sql.BroccoliBaseListener;
import io.broccoli.sql.BroccoliParser;
import io.broccoli.sql.ast.SelectStatementAST;

import static io.broccoli.sql.parser.ParserASTConversions.toAST;

/**
 * @author nicola
 * @since 25/06/2017
 */
class BroccoliQueryListener extends BroccoliBaseListener {

    private SelectStatementAST select;

    @Override
    public void enterSelectStatement(BroccoliParser.SelectStatementContext ctx) {
        select = toAST(ctx);
    }

    /*
     * Build
     */

    public SelectStatementAST build() {
        return select;
    }

}
