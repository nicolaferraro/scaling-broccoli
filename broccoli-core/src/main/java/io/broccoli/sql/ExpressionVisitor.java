package io.broccoli.sql;

import java.math.BigDecimal;

import io.broccoli.sql.ast.ExpressionAST;
import io.broccoli.sql.ast.ExpressionColumnAST;
import io.broccoli.sql.ast.ExpressionLiteralAST;
import io.broccoli.sql.ast.ExpressionBinaryOperationAST;
import io.broccoli.sql.ast.ExpressionUnaryOperatorAST;

/**
 * @author nicola
 * @since 21/05/2017
 */
public class ExpressionVisitor extends BroccoliBaseVisitor<ExpressionAST> {

    @Override
    public ExpressionAST visitExprLiteral(BroccoliParser.ExprLiteralContext ctx) {
        return ctx.literalValue().accept(this);
    }

    @Override
    public ExpressionAST visitExprColumn(BroccoliParser.ExprColumnContext ctx) {
        ExpressionColumnAST column = new ExpressionColumnAST();
        column.setColumnName(ctx.columnName().getText());
        if (ctx.tableName() != null) {
            column.setTableName(ctx.tableName().getText());
        }
        return column;
    }

    @Override
    public ExpressionAST visitExprUnary(BroccoliParser.ExprUnaryContext ctx) {
        ExpressionUnaryOperatorAST unary = new ExpressionUnaryOperatorAST();
        if (ctx.unaryOperator().MINUS() != null) {
            unary.setOperator(ExpressionUnaryOperatorAST.UnaryOperatorAST.MINUS);
        } else if (ctx.unaryOperator().PLUS() != null) {
            unary.setOperator(ExpressionUnaryOperatorAST.UnaryOperatorAST.PLUS);
        } else if (ctx.unaryOperator().K_NOT() != null) {
            unary.setOperator(ExpressionUnaryOperatorAST.UnaryOperatorAST.NOT);
        } else {
            throw new IllegalStateException("Unknown operator");
        }
        unary.setSubExpression(ctx.expr().accept(this));
        return unary;
    }

    @Override
    public ExpressionAST visitExprMultDivMod(BroccoliParser.ExprMultDivModContext ctx) {
        ExpressionBinaryOperationAST op = new ExpressionBinaryOperationAST();
        op.setLeftExpression(ctx.expr(0).accept(this));
        op.setRightExpression(ctx.expr(1).accept(this));
        if (ctx.DIV() != null) {
            op.setOperation(ExpressionBinaryOperationAST.BinaryOperationAST.DIV);
        } else if (ctx.STAR() != null) {
            op.setOperation(ExpressionBinaryOperationAST.BinaryOperationAST.MULT);
        } else if (ctx.MOD() != null) {
            op.setOperation(ExpressionBinaryOperationAST.BinaryOperationAST.MOD);
        } else {
            throw new IllegalStateException("Unknown operation");
        }
        return op;
    }

    @Override
    public ExpressionAST visitExprPlusMinus(BroccoliParser.ExprPlusMinusContext ctx) {
        ExpressionBinaryOperationAST op = new ExpressionBinaryOperationAST();
        op.setLeftExpression(ctx.expr(0).accept(this));
        op.setRightExpression(ctx.expr(1).accept(this));
        if (ctx.PLUS() != null) {
            op.setOperation(ExpressionBinaryOperationAST.BinaryOperationAST.PLUS);
        } else if (ctx.MINUS() != null) {
            op.setOperation(ExpressionBinaryOperationAST.BinaryOperationAST.MINUS);
        } else {
            throw new IllegalStateException("Unknown operation");
        }
        return op;
    }

    @Override
    public ExpressionAST visitExprComparison(BroccoliParser.ExprComparisonContext ctx) {
        ExpressionBinaryOperationAST op = new ExpressionBinaryOperationAST();
        op.setLeftExpression(ctx.expr(0).accept(this));
        op.setRightExpression(ctx.expr(1).accept(this));
        if (ctx.LT() != null) {
            op.setOperation(ExpressionBinaryOperationAST.BinaryOperationAST.LT);
        } else if (ctx.LT_EQ() != null) {
            op.setOperation(ExpressionBinaryOperationAST.BinaryOperationAST.LTE);
        } if (ctx.GT() != null) {
            op.setOperation(ExpressionBinaryOperationAST.BinaryOperationAST.GT);
        } else if (ctx.GT_EQ() != null) {
            op.setOperation(ExpressionBinaryOperationAST.BinaryOperationAST.GTE);
        } else {
            throw new IllegalStateException("Unknown operation");
        }
        return op;
    }

    @Override
    public ExpressionAST visitExprEquality(BroccoliParser.ExprEqualityContext ctx) {
        ExpressionBinaryOperationAST op = new ExpressionBinaryOperationAST();
        op.setLeftExpression(ctx.expr(0).accept(this));
        op.setRightExpression(ctx.expr(1).accept(this));
        if (ctx.EQ() != null || ctx.ASSIGN() != null || (ctx.K_IS() != null && ctx.K_NOT() == null)) {
            op.setOperation(ExpressionBinaryOperationAST.BinaryOperationAST.EQ);
        } else if (ctx.NOT_EQ1() != null || ctx.NOT_EQ2() != null || (ctx.K_IS() != null && ctx.K_NOT() != null)) {
            op.setOperation(ExpressionBinaryOperationAST.BinaryOperationAST.NOT_EQ);
        } if (ctx.K_LIKE() != null) {
            op.setOperation(ExpressionBinaryOperationAST.BinaryOperationAST.LIKE);
        } else {
            throw new IllegalStateException("Unknown operation");
        }
        return op;
    }

    @Override
    public ExpressionAST visitExprAnd(BroccoliParser.ExprAndContext ctx) {
        ExpressionBinaryOperationAST op = new ExpressionBinaryOperationAST();
        op.setLeftExpression(ctx.expr(0).accept(this));
        op.setRightExpression(ctx.expr(1).accept(this));
        op.setOperation(ExpressionBinaryOperationAST.BinaryOperationAST.AND);
        return op;
    }

    @Override
    public ExpressionAST visitExprOr(BroccoliParser.ExprOrContext ctx) {
        ExpressionBinaryOperationAST op = new ExpressionBinaryOperationAST();
        op.setLeftExpression(ctx.expr(0).accept(this));
        op.setRightExpression(ctx.expr(1).accept(this));
        op.setOperation(ExpressionBinaryOperationAST.BinaryOperationAST.OR);
        return op;
    }

    @Override
    public ExpressionAST visitExprParenthesis(BroccoliParser.ExprParenthesisContext ctx) {
        return ctx.expr().accept(this);
    }

    @Override
    public ExpressionAST visitExprNullCheck(BroccoliParser.ExprNullCheckContext ctx) {
        ExpressionUnaryOperatorAST op = new ExpressionUnaryOperatorAST();
        if (ctx.K_ISNULL() != null) {
            op.setOperator(ExpressionUnaryOperatorAST.UnaryOperatorAST.IS_NULL);
        } else if (ctx.K_NOTNULL() != null || (ctx.K_NOT() != null && ctx.K_NULL() != null)) {
            op.setOperator(ExpressionUnaryOperatorAST.UnaryOperatorAST.IS_NOT_NULL);
        } else {
            throw new IllegalStateException("Unknown operator");
        }
        return op;
    }

    // --------------------------------

    @Override
    public ExpressionAST visitLiteralValue(BroccoliParser.LiteralValueContext ctx) {
        ExpressionLiteralAST literal = new ExpressionLiteralAST();
        if (ctx.NUMERIC_LITERAL() != null) {
            literal.setNumericValue(new BigDecimal(ctx.NUMERIC_LITERAL().getText()));
        } else if (ctx.STRING_LITERAL() != null) {
            literal.setStringValue(ctx.STRING_LITERAL().getText());
        }
        return literal;
    }

}
