package com.iceberglearn.expressions;

/** Push NOT nodes to predicate leaves before projection, using De Morgan's laws. */
public final class RewriteNot {

    private RewriteNot() {
    }

    public static Expr rewrite(Expr expression) {
        return rewrite(expression, false);
    }

    private static Expr rewrite(Expr expression, boolean negate) {
        if (expression instanceof Not not) {
            return rewrite(not.child(), !negate);
        } else if (expression instanceof And and) {
            return negate
                    ? Exprs.or(rewrite(and.left(), true), rewrite(and.right(), true))
                    : Exprs.and(rewrite(and.left(), false), rewrite(and.right(), false));
        } else if (expression instanceof Or or) {
            return negate
                    ? Exprs.and(rewrite(or.left(), true), rewrite(or.right(), true))
                    : Exprs.or(rewrite(or.left(), false), rewrite(or.right(), false));
        } else if (!negate) {
            return expression;
        } else if (expression == AlwaysTrue.INSTANCE) {
            return AlwaysFalse.INSTANCE;
        } else if (expression == AlwaysFalse.INSTANCE) {
            return AlwaysTrue.INSTANCE;
        } else if (expression instanceof Equal p) {
            return Exprs.notEqual(p.fieldId(), p.value());
        } else if (expression instanceof NotEqual p) {
            return Exprs.equal(p.fieldId(), p.value());
        } else if (expression instanceof LessThan p) {
            return Exprs.greaterThanOrEqual(p.fieldId(), p.value());
        } else if (expression instanceof LessThanOrEqual p) {
            return Exprs.greaterThan(p.fieldId(), p.value());
        } else if (expression instanceof GreaterThan p) {
            return Exprs.lessThanOrEqual(p.fieldId(), p.value());
        } else if (expression instanceof GreaterThanOrEqual p) {
            return Exprs.lessThan(p.fieldId(), p.value());
        }
        throw new IllegalArgumentException("Unknown expression: " + expression);
    }
}
