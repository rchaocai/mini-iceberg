package com.iceberglearn.expressions;

import com.iceberglearn.partition.PartitionSpec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Evaluate a projected expression against one exact partition tuple. */
public final class PartitionEvaluator {

    private PartitionEvaluator() {
    }

    public static boolean evaluate(Expr expression, PartitionSpec spec, List<Object> tuple) {
        Map<Integer, Object> values = new HashMap<>();
        for (int i = 0; i < spec.fields().size() && i < tuple.size(); i++) {
            values.put(spec.fields().get(i).fieldId(), tuple.get(i));
        }
        return evaluate(expression, values);
    }

    private static boolean evaluate(Expr expression, Map<Integer, Object> values) {
        if (expression == AlwaysTrue.INSTANCE) {
            return true;
        } else if (expression == AlwaysFalse.INSTANCE) {
            return false;
        } else if (expression instanceof And and) {
            return evaluate(and.left(), values) && evaluate(and.right(), values);
        } else if (expression instanceof Or or) {
            return evaluate(or.left(), values) || evaluate(or.right(), values);
        } else if (expression instanceof Not not) {
            return !evaluate(not.child(), values);
        } else if (expression instanceof Equal p) {
            if (!values.containsKey(p.fieldId())) {
                return true;
            }
            return compare(values.get(p.fieldId()), p.value()) == 0;
        } else if (expression instanceof NotEqual p) {
            if (!values.containsKey(p.fieldId())) {
                return true;
            }
            return compare(values.get(p.fieldId()), p.value()) != 0;
        } else if (expression instanceof LessThan p) {
            if (!values.containsKey(p.fieldId())) {
                return true;
            }
            return compare(values.get(p.fieldId()), p.value()) < 0;
        } else if (expression instanceof LessThanOrEqual p) {
            if (!values.containsKey(p.fieldId())) {
                return true;
            }
            return compare(values.get(p.fieldId()), p.value()) <= 0;
        } else if (expression instanceof GreaterThan p) {
            if (!values.containsKey(p.fieldId())) {
                return true;
            }
            return compare(values.get(p.fieldId()), p.value()) > 0;
        } else if (expression instanceof GreaterThanOrEqual p) {
            if (!values.containsKey(p.fieldId())) {
                return true;
            }
            return compare(values.get(p.fieldId()), p.value()) >= 0;
        }
        throw new IllegalArgumentException("Unknown expression: " + expression);
    }

    @SuppressWarnings("unchecked")
    private static int compare(Object actual, Object expected) {
        if (actual == null || expected == null) {
            return actual == expected ? 0 : (actual == null ? -1 : 1);
        }
        Comparable<Object> left = Expr.toComparable(actual);
        Comparable<Object> right = Expr.toComparable(expected);
        return left.compareTo(right);
    }
}
