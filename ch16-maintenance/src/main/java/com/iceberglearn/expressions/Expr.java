package com.iceberglearn.expressions;

import com.iceberglearn.metrics.DataFileStats;

/**
 * A boolean expression tree over a data file's column metrics.
 *
 * <p>The tree has two kinds of nodes:
 * <ul>
 *   <li><b>Leaves</b> ({@link LessThan}, {@link Equal}, {@link GreaterThan}, ...) compare one column
 *       (identified by field id) against a literal.</li>
 *   <li><b>Combinators</b> ({@link And}, {@link Or}, {@link Not}) glue subtrees together.</li>
 * </ul>
 *
 * <p>{@link #evaluate(DataFileStats)} answers a deliberately weak question: <i>might</i> this file
 * contain a row matching the predicate? Returning {@code true} means "maybe — keep the file";
 * returning {@code false} means "definitely not — skip it". This inclusive semantics is what lets
 * us prune files from the manifest without ever opening them, while staying correct: when in
 * doubt, we read.
 *
 * <p>The set of implementations is closed with {@code sealed} so the compiler enforces that
 * every combinator's recursion covers every leaf type — mirroring the {@link com.iceberglearn.transforms.Transform} sealed
 * interface from Chapter 8.
 */
public sealed interface Expr
        permits And, Or, Not, AlwaysTrue, AlwaysFalse,
        LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual, Equal, NotEqual {

    /**
     * Whether this file <i>might</i> contain a matching row.
     *
     * @param stats the file's column metrics; never {@code null} (use {@link AlwaysTrue} for absent
     *              filters)
     * @return {@code true} if the file may match (keep), {@code false} if it cannot match (skip)
     */
    boolean evaluate(DataFileStats stats);

    /**
     * Normalize a value pulled from {@link DataFileStats} (or supplied by the caller) into a
     * {@link Comparable} so that {@code >}, {@code <}, {@code =} can be evaluated uniformly.
     *
     * <p>The two sources of values use different Java types for the same logical number:
     * {@code DataFile.lowerBounds} stores everything as {@code Long}, while a caller writing
     * {@code Exprs.gt(5, 50)} passes an {@code int} literal that boxes to {@code Integer}.
     * Comparing a {@code Long} to an {@code Integer} via {@link Comparable#compareTo} throws
     * {@link ClassCastException}, so we lift {@code Integer} to {@code Long} first.
     */
    @SuppressWarnings("unchecked")
    static <T> Comparable<T> toComparable(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Integer i) {
            return (Comparable<T>) (Long) (long) i;
        }
        return (Comparable<T>) obj;
    }
}
