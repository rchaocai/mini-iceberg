package com.iceberglearn.expressions;

import com.iceberglearn.partition.PartitionField;
import com.iceberglearn.partition.PartitionSpec;

import java.util.List;

/** Project predicates on table rows to predicates on partition tuples. */
public final class Projections {

    private Projections() {
    }

    public static ProjectionEvaluator inclusive(PartitionSpec spec) {
        return new InclusiveProjection(spec);
    }

    public interface ProjectionEvaluator {
        Expr project(Expr expression);
    }

    private static final class InclusiveProjection implements ProjectionEvaluator {
        private final PartitionSpec spec;

        private InclusiveProjection(PartitionSpec spec) {
            this.spec = spec;
        }

        @Override
        public Expr project(Expr expression) {
            return projectRewritten(RewriteNot.rewrite(expression));
        }

        private Expr projectRewritten(Expr expression) {
            if (expression == AlwaysTrue.INSTANCE || expression == AlwaysFalse.INSTANCE) {
                return expression;
            } else if (expression instanceof And and) {
                return Exprs.and(projectRewritten(and.left()), projectRewritten(and.right()));
            } else if (expression instanceof Or or) {
                return Exprs.or(projectRewritten(or.left()), projectRewritten(or.right()));
            } else if (expression instanceof Not) {
                throw new IllegalStateException("NOT must be rewritten before projection");
            }
            return projectPredicate(expression);
        }

        private Expr projectPredicate(Expr predicate) {
            int sourceId = sourceFieldId(predicate);
            List<PartitionField> fields = spec.getFieldsBySourceId(sourceId);
            if (fields.isEmpty()) {
                return AlwaysTrue.INSTANCE;
            }

            Expr result = AlwaysTrue.INSTANCE;
            for (PartitionField field : fields) {
                Expr projected = field.transform().project(field.fieldId(), predicate);
                if (projected != null) {
                    // One source column may feed day(ts) and hour(ts); both constraints are useful.
                    result = Exprs.and(result, projected);
                }
            }
            return result;
        }

        private int sourceFieldId(Expr predicate) {
            if (predicate instanceof Equal p) {
                return p.fieldId();
            } else if (predicate instanceof NotEqual p) {
                return p.fieldId();
            } else if (predicate instanceof LessThan p) {
                return p.fieldId();
            } else if (predicate instanceof LessThanOrEqual p) {
                return p.fieldId();
            } else if (predicate instanceof GreaterThan p) {
                return p.fieldId();
            } else if (predicate instanceof GreaterThanOrEqual p) {
                return p.fieldId();
            }
            throw new IllegalArgumentException("Expected a leaf predicate: " + predicate);
        }
    }
}
